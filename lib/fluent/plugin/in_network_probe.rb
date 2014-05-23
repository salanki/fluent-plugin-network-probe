module Fluent
  class NetworkProbeInput < Input
    Fluent::Plugin.register_input("network_probe", self)

    config_param :probe_type
    config_param :target

    config_param :interval,          :default => 60               # sec

    config_param :fping_count,       :default => 20                # number
    config_param :fping_timeout,     :default => 2                # sec
    config_param :fping_interval,    :default => 1                # sec
    config_param :fping_exec,        :default => '/sbin/ping'

    config_param :hping_count,       :default => 20                # number
    config_param :hping_interval,    :default => 700000                # usec
    config_param :hping_exec,        :default => '/usr/local/sbin/hping'
    config_param :hping_mode,        :default => '-p 80 -S'
    config_param :sudo_exec,         :default => '/usr/bin/sudo'

    config_param :curl_protocol,     :default => 'http'           # http or https
    config_param :curl_port,         :default => 80               # number
    config_param :curl_path,         :default => '/'              # path
    config_param :curl_count,        :default => 5                # number
    config_param :curl_timeout,      :default => 2                # sec
    config_param :curl_interval,     :default => 1                # sec
    config_param :curl_exec,         :default => '/usr/bin/curl'

    config_param :tag,               :default => "network_probe"
    config_param :debug_mode,        :default => false

    def initialize
      require "eventmachine"

      super
    end

    def configure(conf)
      super

      @conf = conf
    end

    def start
      super
      @thread = Thread.new(&method(:run))
      $log.info "starting network probe, target #{@target} probe_type #{@probe_type}."
    end

    def shutdown
      super
      @thread.kill
    end

    def run
      init_eventmachine
      EM.run do
        EM.add_periodic_timer(@interval) do
          begin
            EM.defer do
              Engine.emit("#{@tag}_#{@target}", Engine.now, exec_fping) if @probe_type == 'fping'
            end
            EM.defer do
              Engine.emit("#{@tag}_#{@target}", Engine.now, exec_curl) if @probe_type == 'curl'
            end
            EM.defer do
              Engine.emit("#{@tag}_#{@target}", Engine.now, exec_hping) if @probe_type == 'hping'
            end
          rescue => ex
            $log.warn("EM.periodic_timer loop error.")
            $log.warn("#{ex}, tracelog : \n#{ex.backtrace.join("\n")}")
          end
        end
      end
    end

    def exec_fping
      cmd = "#{@fping_exec} -i #{@fping_interval} -c #{@fping_count} #{@target}"

      cmd_results = run_cmd(cmd)

      round_trip_times = Hash.new(nil)
      
      round_trip_times[:min]= nil
      round_trip_times[:max]= nil
      round_trip_times[:avg]= nil
      
#      $log.info(cmd_results[0])
      
      cmd_results[0].split("\n").each do |line|
        if /\d+ packets transmitted, \d+ packets received, ([\d\.]+)% packet loss/ =~ line
           round_trip_times[:loss] = $1.to_f
        end
        if /round-trip min\/avg\/max\/stddev = ([\d\.]+)\/([\d\.]+)\/([\d\.]+)\/[\d\.]+ ms/=~ line
           round_trip_times[:min]= $1.to_f
           round_trip_times[:avg]= $2.to_f
           round_trip_times[:max]= $3.to_f
        end
      end

      round_trip_times
    end

    def exec_hping
      cmd = "#{@sudo_exec} #{@hping_exec} #{@hping_mode} -i u#{@hping_interval} -c #{@hping_count} #{@target} 2>/dev/stdout"

      cmd_results = run_cmd(cmd)
#      $log.info(cmd_results[0])
      round_trip_times = Hash.new(nil)

      round_trip_times[:min]= nil
      round_trip_times[:max]= nil
      round_trip_times[:avg]= nil
      round_trip_times[:loss] = nil

      cmd_results[0].split("\n").each do |line|
        if /\d+ packets tramitted, \d+ packets received, ([\d\.]+)% packet loss/ =~ line # Yes, the guy who wrote hping can't spell
           round_trip_times[:loss] = $1.to_f
        end
        if /round-trip min\/avg\/max = ([\d\.]+)\/([\d\.]+)\/([\d\.]+) ms/=~ line
           round_trip_times[:min]= $1.to_f
           round_trip_times[:avg]= $2.to_f
           round_trip_times[:max]= $3.to_f
        end
      end

      round_trip_times
    end   



    def exec_curl
      cmd = "#{@curl_exec} #{@curl_protocol}://#{@target}:#{@curl_port}#{@curl_path}  -o/dev/null -w '\%\{time_total\} \%\{size_downloaded\}' -m #{@curl_timeout}"

      result_times = []
      size = 0

      @curl_count.to_i.times do
        cmd_results = run_cmd(cmd)
        parts = cmd_results[0].split("\n").last.split
        result_times << parts[0].to_f * 1000
        size = parts[1].to_i
        sleep @curl_interval.to_i
      end

      results = {}

      results[:max] = result_times.max
      results[:min] = result_times.min
      results[:avg] = result_times.inject(0.0){|r,i| r+=i }/result_times.size
      results[:size] = size
      results[:max_bps] = (size*8)/(result_times.max/1000)
      results[:min_bps] = (size*8)/(result_times.min/1000)
      results[:avg_bps] = (size*8)/(results[:avg]/1000)
      
      results
    end

    private

    def init_eventmachine
      unless EM.reactor_running?
        EM.epoll; EM.kqueue
        EM.error_handler do |ex|
          $log.fatal("Eventmachine problem")
          $log.fatal("#{ex}, tracelog : \n#{ex.backtrace.join("\n")}")
        end
      end
    end

    def run_cmd(cmd)
      require "open3"
      begin
        results = Open3.capture3(cmd)
        return results
      rescue =>e
        $log.warn "[SystemCommond]E:" + e
        return false
      end
    end

  end
end
