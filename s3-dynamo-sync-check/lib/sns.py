from awsobject import AWSObject


class SNS(AWSObject):

    def __str__(self):
        return "sns"

    def send(self, topic_name, subject, message):
        if len(subject) > 100:
            raise ValueError("Subject is too long: %s" % subject)

        topics = self.conn.get_all_topics()
        mytopics = topics["ListTopicsResponse"]["ListTopicsResult"]["Topics"]
        topic_arn = None
        for topic in mytopics:
            if topic_name in topic['TopicArn']:
                topic_arn = topic['TopicArn']
                break
        if topic_arn:
            print "SNS sent by %s" % topic_name
            res = self.conn.publish(topic_arn, message, subject)