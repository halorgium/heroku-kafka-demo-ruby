require 'kafka'
require 'sinatra'
require 'json'
require 'active_support/notifications'
require 'tempfile'
require 'pry'

ADDON_ATTACHMENT = ENV.fetch("ADDON_ATTACHMENT", "KAFKA")
KAFKA_TOPIC = ENV.fetch("KAFKA_TOPIC", "messages")
GROUP_ID = ENV.fetch("KAFKA_CONSUMER_GROUP", "heroku-kafka-demo")

def with_prefix(name)
  "#{ENV["#{ADDON_ATTACHMENT}_PREFIX"]}#{name}"
end

def connect_to_cluster
  # This demo app connects to kafka on multiple threads.
  # Right now ruby-kafka isn't thread safe, so we establish a new client
  # for the consumer and a different one for the consumer.
  Kafka.new(
    seed_brokers: ENV.fetch("#{ADDON_ATTACHMENT}_URL"),
    ssl_ca_cert_file_path: $tmp_ca_file.path,
    ssl_client_cert: ENV.fetch("#{ADDON_ATTACHMENT}_CLIENT_CERT"),
    ssl_client_cert_key: ENV.fetch("#{ADDON_ATTACHMENT}_CLIENT_CERT_KEY"),
  )
end

def initialize_kafka
  $tmp_ca_file = Tempfile.new('ca_certs')
  $tmp_ca_file.write(ENV.fetch("#{ADDON_ATTACHMENT}_TRUSTED_CERT"))
  $tmp_ca_file.close

  $producer = connect_to_cluster.async_producer(delivery_interval: 1)

  # Connect a consumer. Consumers in Kafka have a "group" id, which
  # denotes how consumers balance work. Each group coordinates
  # which partitions to process between its nodes.
  # For the demo app, there's only one group, but a production app
  # could use separate groups for e.g. processing events and archiving
  # raw events to S3 for longer term storage
  $consumer = connect_to_cluster.consumer(group_id: with_prefix(GROUP_ID))
  $recent_messages = []
  #start_consumer
  #start_metrics

  at_exit do
    $producer.shutdown
    $tmp_ca_file.unlink
  end
end

get '/' do
  erb :index
end

get '/cert-check' do
  content_type :json

  cluster = $producer.instance_variable_get(:@worker).instance_variable_get(:@producer).instance_variable_get(:@cluster)
  broker_pool = cluster.instance_variable_get(:@broker_pool)

  brokers = cluster.instance_variable_get(:@seed_brokers).map do |node|
    test_connection(node, broker_pool)
  end

  {attachment: ADDON_ATTACHMENT, brokers: brokers}.to_json
end

def test_connection(node, broker_pool)
  broker = broker_pool.connect(node.hostname, node.port)
  cluster_info = broker.fetch_metadata(topics: [])
  {ok?: true, hostname: node.hostname, topics: cluster_info.topics.map(&:topic_name)}
rescue OpenSSL::SSL::SSLError => e
  {ok?: false, hostname: node.hostname, exception: e.inspect}
end

# This endpoint accesses in memory state gathered
# by the consumer, which holds the last 10 messages received
get '/messages' do
  content_type :json
  $recent_messages.map do |message, metadata|
    {
      offset: message.offset,
      partition: message.partition,
      message: message.value,
      topic: KAFKA_TOPIC
    }
  end.to_json
end

# A sample producer endpoint.
# It receives messages as http bodies on /messages,
# and posts them directly to a Kafka topic.
post '/messages' do
  if request.body.size > 0
    request.body.rewind
    message = request.body.read
    $producer.produce(JSON.parse(message)['message'], topic: with_prefix(KAFKA_TOPIC))
    "received_message: #{message}"
  else
    status 400
    "message was empty"
  end
end

# The consumer subscribes to the topic, and keeps the last 10 messages
# received in memory, so the webapp can send them back over the api.
#
# Consumer group management in Kafka means that this app won't work correctly
# if you run more than one dyno - Kafka will balance out the consumed partitions between
# processes, and the web API will return reads from arbitrary workers, which will be incorrect.
def start_consumer
  Thread.new do
    $consumer.subscribe(with_prefix(KAFKA_TOPIC))
    begin
      $consumer.each_message do |message|
        $recent_messages << [message, {received_at: Time.now.iso8601}]
        $recent_messages.uniq {|m| [m.first.offset, m.first.partition, m.first.topic] }
        $recent_messages.shift if $recent_messages.length > 10
        puts "consumer received message! local message count: #{$recent_messages.size} offset=#{message.offset}"
      end
    rescue Exception => e
      puts "CONSUMER ERROR"
      puts "#{e}\n#{e.backtrace.join("\n")}"
      exit(1)
    end
  end
end

# ruby-kafka exposes metrics over ActiveSupport::Notifications.
# This demo app just logs them, but you could send them to librato or
# another metrics service for graphing.
def start_metrics
  Thread.new do
    ActiveSupport::Notifications.subscribe(/.*\.kafka$/) do |*args|
      event = ActiveSupport::Notifications::Event.new(*args)
      formatted = event.payload.map {|k,v| "#{k}=#{v}"}.join(' ')
      puts "at=#{event.name} #{formatted}"
    end
  end
end
