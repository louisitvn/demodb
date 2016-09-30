$prefix = 'acelle_'


require 'sqlite3'
require 'rubygems'
require 'active_record'
require 'securerandom'
require 'ipaddr'
require 'csv'

class RouletteWheel
  def self.generate(a)
    sum = 0
    total = a.values.inject(0, :+)
    r = rand(total)

    a.each {|key, fitness|
        newsum = sum + fitness
        return key if r >= sum && r < newsum
        sum = newsum
    }
  end

  def self.likely(likelihood)
    return rand < likelihood
  end

  def self.with(possibility)
    return yield if rand < possibility
  end
end


ActiveRecord::Base.establish_connection(
  adapter: 'mysql2',
  encoding: 'utf8',
  host: 'localhost',
  database: 'acelle_demo',
  username: 'acelle_demo',
  password: 'acelle_demo',
  port: 3306,
  timeout: 15000 # You'll only get the BusyException if it takes longer than 15 seconds.
)

ActiveRecord::Base.class_eval do
  self.table_name_prefix = $prefix
end

class TrackingLog < ActiveRecord::Base
  has_many :open_logs, :primary_key => 'message_id', :foreign_key => 'message_id', :dependent => :delete_all
  has_many :click_logs, :primary_key => 'message_id', :foreign_key => 'message_id', :dependent => :delete_all
  has_many :bounce_logs, :primary_key => 'message_id', :foreign_key => 'message_id', :dependent => :delete_all
  has_many :feedback_logs, :primary_key => 'message_id', :foreign_key => 'message_id', :dependent => :delete_all
  belongs_to :campaign
  belongs_to :list
  belongs_to :subscriber
end
class OpenLog < ActiveRecord::Base
  belongs_to :tracking_log
end
class ClickLog < ActiveRecord::Base;end
class BounceLog < ActiveRecord::Base;end
class FeedbackLog < ActiveRecord::Base;end
class UnsubscribeLog < ActiveRecord::Base;end
class Link < ActiveRecord::Base;end
class CampaignLink < ActiveRecord::Base;end
class Blacklist < ActiveRecord::Base
  belongs_to :tracking_logs
end
class SendingServer < ActiveRecord::Base
  self.inheritance_column = '_type'
end
class MailList < ActiveRecord::Base
  has_many :subscribers, :dependent => :delete_all
end
class IpLocation < ActiveRecord::Base
end
class Subscriber < ActiveRecord::Base
  has_many :subscriber_fields
  accepts_nested_attributes_for :subscriber_fields
end
class SubscriberField < ActiveRecord::Base
  belongs_to :subscriber
end
class Campaign < ActiveRecord::Base
  self.inheritance_column = '_type'

  has_many :tracking_logs, :dependent => :delete_all
  
  belongs_to :mail_list
end

# Sample data
$agents = ["Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36", "Mozilla/5.0 (Windows NT10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36", "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:48.0) Gecko/20100101 Firefox/48.0", "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:48.0) Gecko/20100101 Firefox/48.0", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/601.7.8 (KHTML, like Gecko) Version/9.1.3 Safari/601.7.8", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36", "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.11; rv:48.0) Gecko/20100101 Firefox/48.0", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/601.7.7 (KHTML, like Gecko) Version/9.1.2 Safari/601.7.7", "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:48.0) Gecko/20100101 Firefox/48.0", "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12) AppleWebKit/602.1.50 (KHTML, like Gecko) Version/10.0 Safari/602.1.50", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36", "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36", "Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36", "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36", "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/602.1.50 (KHTML, like Gecko) Version/10.0 Safari/602.1.50", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36", "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36", "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2486.0 Safari/537.36 Edge/13.10586", "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36", "Mozilla/5.0 (X11; Linux x86_64; rv:45.0) Gecko/20100101 Firefox/45.0", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/601.6.17 (KHTML, like Gecko) Version/9.1.1 Safari/601.6.17", "Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko", "Mozilla/5.0 (Windows NT 6.3; WOW64; rv:48.0) Gecko/20100101 Firefox/48.0", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36", "Mozilla/5.0 (Windows NT 6.1; rv:48.0) Gecko/20100101 Firefox/48.0", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.79 Safari/537.36 Edge/14.14393", "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.101 Safari/537.36", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.101 Safari/537.36", "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:47.0) Gecko/20100101 Firefox/47.0", "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:49.0) Gecko/20100101 Firefox/49.0", "Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko", "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/601.7.8 (KHTML, like Gecko) Version/9.1.3 Safari/601.7.8", "Mozilla/5.0 (iPad; CPU OS 9_3_5 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13G36 Safari/601.1", "Mozilla/5.0 (iPhone; CPU iPhone OS 10_0_1 like Mac OS X) AppleWebKit/602.1.50 (KHTML, like Gecko) Version/10.0 Mobile/14A403 Safari/602.1", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.10; rv:48.0) Gecko/20100101 Firefox/48.0", "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:47.0) Gecko/20100101 Firefox/47.0", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.101 Safari/537.36", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.89 Safari/537.36", "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:48.0) Gecko/20100101 Firefox/48.0", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/52.0.2743.116 Chrome/52.0.2743.116 Safari/537.36", "Mozilla/5.0 (X11; Linux x86_64; rv:48.0) Gecko/20100101 Firefox/48.0", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.101 Safari/537.36", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.89 Safari/537.36", "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:45.0) Gecko/20100101 Firefox/45.0", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36", "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.101 Safari/537.36", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.92 Safari/537.36", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.11; rv:49.0) Gecko/20100101 Firefox/49.0", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/51.0.2704.79 Chrome/51.0.2704.79 Safari/537.36", "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:47.0) Gecko/20100101 Firefox/47.0", "Mozilla/5.0 (X11; Linux x86_64; rv:49.0) Gecko/20100101 Firefox/49.0", "Mozilla/5.0 (iPad; CPU OS 10_0_1 like Mac OS X) AppleWebKit/602.1.50 (KHTML, like Gecko) Version/10.0 Mobile/14A403 Safari/602.1", "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0; Trident/5.0)", "Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_4) AppleWebKit/601.5.17 (KHTML, like Gecko) Version/9.1 Safari/601.5.17", "Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.82 Safari/537.36", "Mozilla/5.0 (Windows NT 10.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.89 Safari/537.36", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/601.7.7 (KHTML, like Gecko) Version/9.1.2 Safari/601.7.7", "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:48.0) Gecko/20100101 Firefox/48.0", "Mozilla/5.0 (X11; Fedora; Linux x86_64; rv:48.0) Gecko/20100101 Firefox/48.0", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.106 Safari/537.36", "Mozilla/5.0 (Windows NT 6.1; rv:49.0) Gecko/20100101 Firefox/49.0", "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:49.0) Gecko/20100101 Firefox/49.0", "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0; Trident/5.0)", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36", "Mozilla/5.0 (Windows NT 5.1; rv:48.0) Gecko/20100101 Firefox/48.0", "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36 OPR/39.0.2256.71", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/602.1.50 (KHTML, like Gecko) Version/10.0 Safari/602.1.50", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.116 Safari/537.36"]
$ips = IpLocation::all.map(&:ip_address)
$links = [{url: 'http://sales.sample.com/promotion'}, {url: 'http://sales.sample.com/search=new'}]

# clean up master data
Link.delete_all
CampaignLink.delete_all
IpLocation.delete_all

# populate the ip_locations table
IpLocation.create(CSV.read('ipdb.csv', headers: true).map{|r|
  #selected_ip = r['ip_from'].to_i + rand(r['ip_to'].to_i - r['ip_from'].to_i)
  {
    ip_address: IPAddr.new(r['ip_from'].to_i, Socket::AF_INET).to_s, 
    country_code: r['country_code'],
    country_name: r['country_name'],
    region_code: 0,
    zipcode: 0,
    metro_code: 0,
    areacode: 0,
    region_name: r['region_name'],
    city: r['city_name'],
    latitude: r['latitude'],
    longitude: r['longitude']
  }
})

IpLocation.all.each do |r|
  selected = RouletteWheel::generate(
    {code: 'US', name: 'United States'} => 30,
    {code: 'JP', name: 'Japan'} => 10,
    {code: 'CN', name: 'China'} => 10,
    {code: r.country_code, name: r.country_name} => 50
  )
  r.country_code = selected[:code]
  r.country_name = selected[:name]
  r.save
end
Link.create($links)


# For every campaign
campaigns = Campaign.where(status: ['ready', 'done'])
campaigns.each do |campaign|
  puts "Working on campaign [#{campaign.name}]"
  
  campaign.tracking_logs.destroy_all
  campaign.mail_list.subscribers.destroy_all

  Link.all.each do |link|
    CampaignLink.create([
      { link_id: link.id, campaign_id: campaign.id }
    ])
  end

  # generate subscriber
  campaign.mail_list.subscribers.create((rand(100) + 100).times.map{ |i| 
    date = rand(4).months.ago
    {
      uid: SecureRandom.hex(10),
      email: "user_#{i}@gmail.com",
      status: RouletteWheel::generate('subscribed' => 81, 'unsubscribed' => 12, 'spam-reported' => 2, 'blacklisted' => 5),
      from: 'unknown',
      ip: '0.0.0.0',
      created_at: date,
      updated_at: date,
      subscriber_fields_attributes: [
        { field_id: 2, value: "User #{i}"}, {field_id: 3, value: "User #{i}" }, {field_id: 1, value: "user_#{i}@gmail.com", created_at: date, updated_at: date}
      ]
    }
  })

  # generate tracking_logs
  campaign.tracking_logs.create(campaign.mail_list.subscribers.where(status: 'subscribed').map{|r|
    message_id = SecureRandom.hex(10)
    { subscriber_id: r.id, sending_server_id: SendingServer::first.id, user_id: campaign.user_id, message_id: message_id, runtime_message_id: message_id, status: RouletteWheel::generate('sent' => 91, 'failed' => 9) }
  })

  # generate tracking_logs
  campaign.tracking_logs.create(campaign.mail_list.subscribers.where("status IN (?)", ['spam-reported', 'blacklisted', 'unsubscribed']).map{|r|
    message_id = SecureRandom.hex(10)
    { subscriber_id: r.id, sending_server_id: SendingServer::first.id, user_id: campaign.user_id, message_id: message_id, runtime_message_id: message_id, status: 'sent' }
  })

  # generate feedback_logs
  FeedbackLog.create(campaign.tracking_logs.where($prefix + 'subscribers' => {status: 'spam-reported'}).joins(:subscriber).map{|r|
    { message_id: r.message_id, runtime_message_id: r.runtime_message_id, feedback_type: 'spam', raw_feedback_content: '' }
  })

  # generate bounce_logs
  BounceLog.create(campaign.tracking_logs.where($prefix + 'subscribers' => {status: 'blacklisted'}).joins(:subscriber).map{|r|
    { message_id: r.message_id, runtime_message_id: r.runtime_message_id, bounce_type: 'spam', raw: '' }
  })



  # open logs
  OpenLog.create(campaign.tracking_logs.where($prefix + 'subscribers' => {status: ['unsubscribed', 'spam-reported']}).joins(:subscriber).map{|r|
    date = rand(24).hours.ago
    { message_id: r.message_id, ip_address: $ips.sample, user_agent: $agents.sample, created_at: date, updated_at: date }
  })

  OpenLog.create(campaign.tracking_logs.where($prefix + 'subscribers' => {status: ['subscribed']}).joins(:subscriber).map{|r|
    RouletteWheel::with(0.6) do 
      date = rand(24).hours.ago
      { message_id: r.message_id, ip_address: $ips.sample, user_agent: $agents.sample, created_at: date, updated_at: date }
    end
  }.reject(&:nil?))

  # open logs
  UnsubscribeLog.create(campaign.tracking_logs.where($prefix + 'subscribers' => {status: ['unsubscribed']}).joins(:subscriber).map{|r|
    date = rand(24).hours.ago
    { message_id: r.message_id, ip_address: $ips.sample, user_agent: $agents.sample, created_at: date, updated_at: date }
  })

  # open logs
  ClickLog.create(campaign.tracking_logs.joins(:open_logs).select($prefix + 'tracking_logs.*', $prefix + 'open_logs.*').map{|r|
    RouletteWheel::with(0.7) do 
      date = rand(24).hours.ago
      { message_id: r.message_id, url: $links.sample[:url], ip_address: r.ip_address, user_agent: r.user_agent, created_at: date, updated_at: date }
    end
  }.reject(&:nil?))
  
  # update campaign status
  campaign.name = 'Product Weekly Newsletter' if campaign.name == 'Untitled'
  campaign.status = 'done'
  campaign.save
end
