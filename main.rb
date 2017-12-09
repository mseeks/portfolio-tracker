require "json"
require "kafka"
require "rest-client"

logger = Logger.new(STDOUT)
logger.level = Logger::WARN

@kafka = Kafka.new(
  logger: logger,
  seed_brokers: ["#{ENV["KAFKA_HOST"]}:#{ENV["KAFKA_PORT"]}"],
  client_id: "portfolio-tracker"
)

@api = RestClient::Resource.new("https://api.robinhood.com")
@api_headers = {
  accept: "application/json",
  Authorization: "Token #{ENV["ROBINHOOD_TOKEN"]}"
}

@account = JSON.parse(@api["accounts/"].get(@api_headers).body)["results"].first
@portfolio = JSON.parse(RestClient.get(@account["portfolio"], @api_headers).body)

portfolio_stats = {
  value: (@portfolio["extended_hours_equity"] || @portfolio["equity"]).to_f.round(2),
  at: Time.now
}.to_json

puts "portfolio-stats -> #{portfolio_stats}"
@kafka.deliver_message(portfolio_stats, topic: "portfolio-stats")
