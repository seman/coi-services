#!/usr/bin/env python

'''
@package ion.processes.data.presentation
@file ion/processes/data/transforms/notification_worker.py
@author Swarbhanu Chatterjee
@brief NotificationWorker Class. An instance of this class acts as an notification worker.
'''

from pyon.public import log, RT, OT, PRED
from pyon.util.async import spawn
from pyon.core.exception import BadRequest, NotFound
from ion.core.process.transform import TransformEventListener
from pyon.event.event import EventSubscriber
from ion.services.dm.utility.uns_utility_methods import send_email, calculate_reverse_user_info
from ion.services.dm.utility.uns_utility_methods import setting_up_smtp_client, check_user_notification_interest
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from collections import defaultdict
from interface.objects import DeliveryModeEnum, NotificationFrequencyEnum, NotificationTypeEnum
from ion.services.sa.observatory.observatory_util import ObservatoryUtil
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient


import gevent, time
from gevent import queue

class NotificationWorker(TransformEventListener):
    """
    Instances of this class acts as a Notification Worker.
    """
    def on_init(self):
        self.user_info = {}
        self.resource_registry = ResourceRegistryServiceClient()
        self.q = gevent.queue.Queue()

        super(NotificationWorker, self).on_init()
        self.subscribers = defaultdict(lambda: dict())
        '''
        Data structure

        {"user_id":
                  {
                      "sub_id" : {
                          "notification": notificationRequest
                          "notification_id": "111",
                          "parent_sub_id": #points to parent sub id. If this is parent, it is set to None

                      },
                      "sub_id2" : {
                          "notification": notificationRequest
                          "notification_id": "111",
                          "parent_sub_id": #points to parent sub id. If this is parent, it is set to None
                      },
                      "sub_id3" : {
                          "notification": notificationRequest
                          "notification_id": "222",
                          "parent_sub_id": #points to parent sub id. If this is parent, it is set to None
                      },
                  },
          "user_id2":
                  {
                      "sub_id" : {
                          "notification": notificationRequest
                          "notification_id": "333",
                          "parent_sub_id": #points to parent sub id. If this is parent, it is set to None

                      },
                      "sub_id2" : {
                          "notification": notificationRequest
                          "notification_id": "333",
                          "parent_sub_id": #points to parent sub id. If this is parent, it is set to None
                      },
                      "sub_id3" : {
                          "notification": notificationRequest
                          "notification_id": "222",
                          "parent_sub_id": #points to parent sub id. If this is parent, it is set to None
                      },
                  },
           }
        '''
        self.subscription = {}

        # Associates events with users
        self.event_subscripers = dict()
        '''
        for each sub id, add user id who are subscribed to the events
        {
            "sub_id": [{"user_id": user_id, "notification_id":"xxx"}, {"user_id": user_id, "notification_id":"xxx"}
            "sub_id2": [{"user_id": user_id, "notification_id":"xxx"}, {"user_id": user_id, "notification_id":"xxx"}

        }
        '''

    def test_hook(self, user_info, reverse_user_info ):
        '''
        This method exists only to facilitate the testing of the reload of the user_info dictionary
        '''
        self.q.put((user_info, reverse_user_info))

    def on_start(self):
        super(NotificationWorker,self).on_start()

        self.reverse_user_info = None
        self.user_info = None

        #------------------------------------------------------------------------------------
        # Start by loading the user info and reverse user info dictionaries
        #------------------------------------------------------------------------------------

        try:
            self.user_info = self.load_user_info()
            self.reverse_user_info =  calculate_reverse_user_info(self.user_info)

            log.debug("On start up, notification workers loaded the following user_info dictionary: %s" % self.user_info)
            log.debug("The calculated reverse user info: %s" % self.reverse_user_info )

        except NotFound as exc:
            if exc.message.find('users_index') > -1:
                log.warning("Notification workers found on start up that users_index have not been loaded yet.")
            else:
                raise NotFound(exc.message)

        #------------------------------------------------------------------------------------
        # Create an event subscriber for Reload User Info events
        #------------------------------------------------------------------------------------

        def reload_user_info(event_msg, headers):
            '''
            Callback method for the subscriber to ReloadUserInfoEvent
            '''

            try:
                self.user_info = self.load_user_info()
            except NotFound:
                log.warning("ElasticSearch has not yet loaded the user_index.")

            self.reverse_user_info = calculate_reverse_user_info(self.user_info)
            self.test_hook(self.user_info, self.reverse_user_info)

            #log.debug("After a reload, the user_info: %s" % self.user_info)
            #log.debug("The recalculated reverse_user_info: %s" % self.reverse_user_info)

        # the subscriber for the ReloadUSerInfoEvent
        self.reload_user_info_subscriber = EventSubscriber(
            event_type=OT.ReloadUserInfoEvent,
            origin='UserNotificationService',
            callback=reload_user_info
        )
        self.add_endpoint(self.reload_user_info_subscriber)

        # the subscriber for the ReloadUSerInfoEvent
        self.cn = EventSubscriber(
            event_type=OT.CreateNotificationEvent,
            origin='UserNotificationService',
            callback=self.create_notification
        )
        self.add_endpoint(self.cn)

        self.dn = EventSubscriber(
            event_type=OT.DeleteNotificationEvent,
            origin='UserNotificationService',
            callback=self.delete_notification
        )
        self.add_endpoint(self.dn)


        # the subscriber for the UserInfo resource update events
        #todo do we need this???
        '''
        self.userinfo_rsc_mod_subscriber = EventSubscriber(
            event_type=OT.ResourceModifiedEvent,
            sub_type="UPDATE",
            origin_type="UserInfo",
            callback=reload_user_info
        )

        self.add_endpoint(self.userinfo_rsc_mod_subscriber)
        '''

    def process_event(self, msg, headers):
        """
        Callback method for the subscriber listening for all events
        """
        #------------------------------------------------------------------------------------
        # From the reverse user info dict find out which users have subscribed to that event
        #------------------------------------------------------------------------------------

        #todo: from self.event_subscripers, get all users who are waiting for this event
        user_ids = []
        if self.reverse_user_info:
            user_ids = check_user_notification_interest(event = msg, reverse_user_info = self.reverse_user_info)

            #log.debug('process_event  user_ids: %s', user_ids)

            #log.debug("Notification worker found interested users %s" % user_ids)

        #------------------------------------------------------------------------------------
        # Send email to the users
        #------------------------------------------------------------------------------------

        for user_id in user_ids:
            msg_recipient = self.user_info[user_id]['user_contact'].email
            self.smtp_client = setting_up_smtp_client()
            send_email(event=msg,
                       msg_recipient=msg_recipient,
                       smtp_client=self.smtp_client,
                       rr_client=self.resource_registry)
            self.smtp_client.quit()

    def get_user_notifications(self, user_info_id=''):
        """
        Get the notification request objects that are subscribed to by the user

        @param user_info_id str

        @retval notifications list of NotificationRequest objects
        """
        notifications = []
        user_notif_req_objs, _ = self.resource_registry.find_objects(
            subject=user_info_id, predicate=PRED.hasNotification, object_type=RT.NotificationRequest, id_only=False)

        #log.debug("get_user_notifications Got %s notifications, for the user: %s", len(user_notif_req_objs), user_info_id)

        for notif in user_notif_req_objs:
            # do not include notifications that have expired
            if notif.temporal_bounds.end_datetime == '':
                notifications.append(notif)

        return notifications

    def load_user_info(self):
        '''
        Method to load the user info dictionary used by the notification workers and the UNS

        @retval user_info dict
        '''

        users, _ = self.resource_registry.find_resources(restype= RT.UserInfo)
        user_info = {}

        if not users:
            return {}

        for user in users:
            notifications_disabled = False
            notifications_daily_digest = False

            notifications = self.get_user_notifications(user_info_id=user)

            for variable in user.variables:
                if type(variable) is dict and variable.has_key('name'):

                    if variable['name'] == 'notifications_daily_digest':
                        notifications_daily_digest = variable['value']

                    if variable['name'] == 'notifications_disabled':
                        notifications_disabled = variable['value']
                else:
                    log.warning('Invalid variables attribute on UserInfo instance. UserInfo: %s', user)

            user_info[user._id] = { 'user_contact' : user.contact, 'notifications' : notifications,
                                    'notifications_daily_digest' : notifications_daily_digest, 'notifications_disabled' : notifications_disabled}


        return user_info

    def create_notification(self, event_msg, headers):
        log.debug('create_notification event_msg: %s ', event_msg)
        # Get NotificationRequest
        n = self.resource_registry.read(object_id=event_msg.notification_id)
        user_id = self._get_user_id_fom_notification_id(notification_id=event_msg.notification_id)

        # check if an aggregate notification is requested, then get all children ids
        children_ids = []
        if n.type != NotificationTypeEnum.SIMPLE and n.origin:
            outil_client = ObservatoryUtil(self)
            children_ids = self._find_children_by_type(parent_id=n.origin, type_=n.type, outil=outil_client)

        # Add parent subscription id
        parent_sub_id = self._add_subscriber(user_id=user_id, resource_id=event_msg.origin, event_type=event_msg.event_type,
                                             notification=n, notification_id=event_msg.notification_id)

        for child_id in children_ids:
            self._add_subscriber(user_id=user_id, resource_id=child_id, event_type=event_msg.event_type,
                                 notification=n, notification_id=event_msg.notification_id, parent_sub_id=parent_sub_id)

    def _add_subscriber(self, user_id, resource_id, event_type, notification, notification_id, parent_sub_id=None):
        sub = self._create_subscription_data(notification=notification, notification_id=notification_id, parent_sub_id=parent_sub_id)
        sub_id = self._create_subscription_id(resource_id, event_type)

        # Each user has dict of subscriptions
        self.subscribers[user_id][sub_id] = sub

        # Associates each event with users/subscribers
        s_id = {"user_id": user_id, "notification_id":notification_id}
        if s_id not in self.event_subscripers[sub_id]:
            self.event_subscripers[sub_id].append(s_id)
        return sub_id

    def _create_subscription_data(self, notification, notification_id, parent_sub_id=None):
        sub = {
            "notification": notification,
            "notification_id": notification_id,
            "parent_sub_id": parent_sub_id
        }
        return sub

    def _get_user_id_fom_notification_id(self, notification_id):
        #todo review
        user_ids, _ = self.resource_registry.find_objects(subject=notification_id, predicate=PRED.hasNotification,
                                                          object_type=RT.NotificationRequest, id_only=False)
        if len(user_ids) > 1:
            raise BadRequest("_get_user_id_fom_notification_id: Multiple  user id for a single notification id ")

        return user_ids[0] if len(user_ids) == 1 else None

    def _create_subscription_id(self, resource_id, event_type):
        return '%s_%s' % (resource_id, event_type)


    def delete_notification(self, event_msg, headers):
        '''
        Callback method for the subscriber to ReloadUserInfoEvent
        '''
        #todo: delete notification from the data structure, self.subscribers and self.event_subscripers
        n_id = event_msg.notification_id
        print "\n\n\n\n <<<< delete notification >>>> \n"
        for k, i in enumerate(self.subscribers):
            # this gives us "user_id: "
            for k1, i2 in enumerate(self.subscribers[i]):
                # sub_id:", i2
                # data:", m[i][i2]
                #
                print "my sub_id:", i2
                print "data:", self.subscribers[i][i2]
                print "notification_id", self.subscribers[i][i2]["notification_id"]



    def _find_children_by_type(self, parent_id='', type_='', outil=None):

        log.debug('_find_children_by_type  parent_id: %s   type_: %s', parent_id, type_)
        child_ids = []

        if type_ == NotificationTypeEnum.PLATFORM:
            device_relations = outil.get_child_devices(parent_id)
            child_ids = [did for pt,did,dt in device_relations[ parent_id] ]
        elif type_ == NotificationTypeEnum.SITE:
            child_site_dict, ancestors = outil.get_child_sites(parent_id)
            child_ids = child_site_dict.keys()

        elif type == NotificationTypeEnum.FACILITY:
            resource_objs, _ = self.resource_registry.find_objects(
                subject=parent_id, predicate=PRED.hasResource, id_only=False)
            for resource_obj in resource_objs:
                if resource_obj.type_ == RT.DataProduct \
                    or resource_obj.type_ == RT.InstrumentSite or resource_obj.type_ == RT.InstrumentDevice \
                    or resource_obj.type_ == RT.PlatformSite or resource_obj.type_ == RT.PlatformDevice:
                    child_ids.append(resource_obj._id)
        if parent_id in child_ids:
            child_ids.remove(parent_id)
        log.debug('_find_children_by_type  child_ids:  %s', child_ids)
        return child_ids

    def _create_subscription_id(self, resource_id, event_type):
        return '%s_%s' % (resource_id, event_type)