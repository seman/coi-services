from interface.services.dm.iuser_notification_service2 import BaseUserNotificationService2
from ion.core.process.transform import TransformEventListener
from pyon.event.event import EventPublisher, EventSubscriber
from pyon.public import RT, PRED, get_sys_name, OT, IonObject

class UserNotification2Service(BaseUserNotificationService2):

    def __init__(self, *args, **kwargs):
        self._schedule_ids = []
        BaseUserNotificationService2.__init__(self, *args, **kwargs)

        #todo: debug only..remove
        print "\n\n\n started user_notification "

    def on_start(self):
        #super(UserNotificationService2,self).on_start()
        self.event_publisher = EventPublisher(process=self)
        pass

    def process_event(self, msg, headers):
        #todo: debug only remove
        print "\n\n << process event::::: name ", self.random_name, "\nmsg: ",  msg

    def on_quit(self):
        print " uns is done"

    def create_notification(self, notification=None, user_id=''):
        print "\n\n\n <<<<< create_notification>>> ", notification
        print "\n\n"
        # todo: add to db

        parent_origin = None
        if notification.child_resource_type:
            #TODO  get all associated resource
            parent_origin = notification.origin
            pass

        notification_id = "111111"
        self.event_publisher.publish_event(
            origin='UserNotification2Service',
            event_type=OT.NotificationCreatedEvent,
            notification_origin=notification.origin,
            notification_id=notification_id,
            notification_origin_type=notification.origin_type,
            notification_event_type=notification.event_type,
            notification_child_resource=notification.child_resource,
            notification_child_resource_type=notification.child_resource_type,
            notification_address=notification.notification_address,
            notification_address_type=notification.notification_address_type,
            notification_frequency=notification.notification_frequency,
            parent_origin=parent_origin,
            user_id=user_id,
            )

        return


    def delete_notification(self, notification_id=''):
        self.event_publisher.publish_event(
            origin='UserNotification2Service',
            event_type=OT.NotificationDeleteEvent,
            notification_id=notification_id,
        )

    def get_user_notifications(self, user_info_id=''):
        print "\n\n\n <<<<< new get_user >>>> ", user_info_id
        print "\n\n"
