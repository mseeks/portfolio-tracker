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
@positions = JSON.parse(RestClient.get(@account["positions"] + "?nonzero=true", @api_headers).body)["results"].map do |position|
  position["instrument"] = JSON.parse(RestClient.get(position["instrument"], @api_headers).body)

  position
end

portfolio_positions = {
  positions: @positions.map{|position|
    current_quote_response = JSON.parse(RestClient.get(position["instrument"]["quote"]).body)
    current_quote = current_quote_response["last_extended_hours_trade_price"] || current_quote_response["last_trade_price"]
    current_quote = current_quote.to_f
    quantity = position["quantity"].to_f
    average_buy_price = position["average_buy_price"].to_f

    {
      average_buy_price: average_buy_price,
      current_quote: current_quote,
      name: position["instrument"]["simple_name"],
      quantity: quantity,
      symbol: position["instrument"]["symbol"]
    }
  },
  at: Time.now
}.to_json

@kafka.deliver_message(portfolio_positions, topic: "portfolio-positions")
