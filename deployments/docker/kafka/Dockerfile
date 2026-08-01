# Used two ways in the k8s gitops repo:
#   - as the KRaft broker image for the kafka StatefulSet (default cp-kafka
#     entrypoint, unmodified)
#   - as the image for the topic-creation Job, which overrides `command` to
#     run /opt/kafka-topics.sh instead of starting a broker
FROM confluentinc/cp-kafka:7.6.1

COPY --chmod=755 kafka-topics.sh /opt/kafka-topics.sh
