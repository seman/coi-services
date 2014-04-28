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
from pyon.event.event import EventSubscriber, EventPublisher
from ion.services.dm.utility.uns_utility_methods import send_email, calculate_reverse_user_info
from ion.services.dm.utility.uns_utility_methods import setting_up_smtp_client, check_user_notification_interest
from interface.services.coi.iresource_registry_service import ResourceRegistryServiceClient
from interface.objects import NotificationRequest2, Event, DeviceStatusType, AggregateStatusType, InformationStatus
from collections import defaultdict

#todo for testing only ##delete##
import json
import pprint



class NotificationWorker2(TransformEventListener):
    def on_init(self):
        self.subscribers = defaultdict(lambda: dict())
        '''
        Data structure

        {"user_id":
                  {
                      "sub_id" : {
                          "notification_address":"myemail@yahoo",
                          "notification_id": "111",
                          "event_type": "ResourceModifiedEvent",
                          "notification_frequency": "realtime",
                           "notification_sent_counts": 0,
                           "children_resource_ids": ["x","y","u"],
                      },
                      "sub_id2" : {
                          "notification_address":"myemail@yahoo",
                          "notification_id": "111",
                          "event_type": "ResourceModifiedEvent",
                          "notification_frequency": "realtime",
                           "notification_sent_counts": 0,
                           "children_resource_ids": ["x","y","u"],
                      },
                  },
          "user_id2":
                  {
                      "sub_id" : {
                          "notification_address":"myemail@yahoo",
                          "notification_id": "111",
                          "event_type": "ResourceModifiedEvent",
                          "notification_frequency": "realtime",
                           "notification_sent_counts": 0,
                           "children_resource_ids": ["x","y","u"],
                      },
                      "sub_id2" : {
                          "notification_address":"myemail@yahoo",
                          "notification_id": "111",
                          "event_type": "ResourceModifiedEvent",
                          "notification_frequency": "realtime",
                           "notification_sent_counts": 0,
                           "children_resource_ids": ["x","y","u"],
                      },
                  },
           }
        '''
        self.subscription = {}
        """
        origin
        event_type
        parent_origin
        email

        """
        self.resource_registry = ResourceRegistryServiceClient()

        super(NotificationWorker2, self).on_init()


    def on_start(self):
        super(NotificationWorker2, self).on_start()
        self.event_publisher = EventPublisher(process=self)

        self.reload_user_info_subscriber = EventSubscriber(
            event_type=OT.NotificationCreatedEvent,
            origin='UserNotification2Service',
            callback=self.notification_event_handler
        )
        self.add_endpoint(self.reload_user_info_subscriber)

    def notification_event_handler(self, event_msg, headers):
        """
        Handles events from user_notification
        """
        print "\nNotification worker: notification_event_handler: ", event_msg

        notification_id = event_msg.notification_id
        user_id = event_msg.user_id
        sub = self._create_subscription_data(
            user_id=user_id,
            notification_id=event_msg.notification_id,
            resource_id=event_msg.notification_origin,
            event_type=event_msg.notification_event_type,
            notification_address=event_msg.notification_address,
            notification_frequency=event_msg.notification_frequency,
            parent_origin=event_msg.parent_origin,
        )
        sub_id = self._create_subscription_id(resource_id=event_msg.notification_origin, event_type=event_msg.notification_event_type)

        if self._is_subscription_exist(user_id, sub_id):
            return

        # Add subscription
        self.subscribers[user_id][sub_id] = sub

        self._list_all_notifications()

    def _create_subscription_id(self, resource_id, event_type):
        return '%s_%s' % (resource_id, event_type)

    def _create_subscription_data(self, user_id, notification_id, resource_id, event_type, notification_address,
                                  notification_frequency, parent_origin=None):
        sub = {"user_id": user_id,
               "notification_id": notification_id,
               "resource_id": resource_id,
               "event_type": event_type,
               "notification_address": notification_address,
               "notification_frequency": notification_frequency,
               "parent_resource_id": parent_origin,
               "notification_count": 0,
               "notification_count_limit": 100,

        }
        return sub

    def _is_subscription_exist(self, user_id, sub_id):
        sub = self.subscribers.get(user_id)
        return False if not sub else sub.get(sub_id)

    def _list_all_notifications(self):
        print "List all notifications: "
        for _, i in enumerate(self.subscribers):
            print "\n\nuser_id:\n--------\n", i
            for _, i2 in enumerate(self.subscribers[i]):
                print "Sub id:   ", i2
                print "sub data: ", self.subscribers[i][i2]
                print "----------"


    def process_event(self, msg, headers):
        """
        Callback method for the subscriber listening for all events
        """
        resource_id = msg._id
        event_type = msg.__class__.__name__
        sub_id = self._create_subscription_id(resource_id=resource_id, event_type=event_type)

        sub = self.filter_event(sub_id)
        if sub:
            self._send_notification(sub)

        #print "\n\n<< process_event process name: ", self._process.name, "\nmsg: ",  msg,
        #print "dddd: ", msg['origin']
        #print "\n\n"

        #msg = msg.replace("'",'"')
        #print pprint.pformat(msg)

    def filter_event(self, sub_id):
        # find all user waiting for this sub id
        for _, user in enumerate(self.subscription):
            if self.subscription[user].get(sub_id):
                #todo: add all sub
                return self.subscription[user][sub_id]
        return False

    def _send_notification(self, sub):
        """
        Enforce limit
        increment count
        sent events to other workers to update the count
        if not real-time, don't send notification
        """

        #Encforce limit
        if sub["notification_count"] > sub["notification_count_limit"]:
            # don't send anything if the counter is more than the limi
            return

        # Increment the counter
        # Todo: don't update the counter here..create event handler to update the counter
        sub["notification_count"] += sub["notification_count"]

        # Send notification
        #todo: debug
        print "Sending notification for: ", sub["user_id"]

        # Publish events to other workers
        #todo:


