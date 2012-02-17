#!/usr/bin/env ruby

require 'date'
require 'logger'
require 'roma/client/rclient'
require 'roma/tools/multi_commander'
require 'optparse'

module Roma
  module Test
    class RomaProc
      attr_accessor :addr
      attr_accessor :port
      attr_accessor :pid

      def initialize a, p
        @addr = a
        @port = p
      end

      def self.to_str procs
        msg = ""
        procs.each { |proc|
          msg = msg + proc.addr + "_" + proc.port.to_s + " "
        }
        msg
      end
    end

    class Stress
      attr :cnt
      attr :tmax
      attr :tmin
      attr :num_of_threads
      attr_accessor :num_of_finish
      attr_accessor :runnable

      def initialize th_num
        @cnt = 0
        @tmax = 0
        @tmin = 100
        @num_of_threads = th_num
        @runnable = true
        @num_of_finish = 0
      end

      def start addr, port, req="random_reqs"
        Thread.new {
          sleep_time=10
          while @runnable
            sleep sleep_time
            printf("qps=%d max=%f min=%f ave=%f\n", @cnt / sleep_time, @tmax, @tmin, sleep_time / @cnt.to_f)
            @@cnt=0
            @@tmax=0
            @@tmin=100
          end
        }

        working_threads = []
        @num_of_threads.times {
          working_threads << Thread.new {
            send(req, addr, port)
          }
        }
      end

      def single_start addr, port, n=1000, req="random_reqs"
        ts = DateTime.now
        send(req, addr, port, n)
        t = (DateTime.now - ts).to_f * 86400.0
        printf("qps=%d max=%f min=%f ave=%f\n", n / t , @tmax, @tmin, t / n.to_f)
        @@cnt=0
        @@tmax=0
        @@tmin=100
      end

      def random_reqs addr, port, n=1000
        rc = Roma::Client::RomaClient.new([ "#{addr}_#{port.to_s}" ])
        while @runnable
          begin 
            i = rand(n)
            ts = DateTime.now
            case rand(3)
            when 0
              res = rc.set(i.to_s, 'hoge' + i.to_s)
              puts "set k=#{i} #{res}" if res==nil || res.chomp != 'STORED'
            when 1
              res = rc.get(i.to_s)
              puts "get k=#{i} #{res}" if res == :error
            when 2
              res = rc.delete(i.to_s)
              puts "del k=#{i} #{res}" if res != 'DELETED' && res != 'NOT_FOUND'
            end
            t = (DateTime.now - ts).to_f * 86400.0
            @tmax=t if t > @tmax
            @tmin=t if t < @tmin
            @cnt+=1
          rescue => e
            p e
          end
        end
      rescue => e
        p e
      end
      private :random_reqs

      def sequential_reqs addr, port, n = 1000
        rc = Roma::Client::RomaClient.new([ "#{addr}_#{port.to_s}" ])
        i = 0
        while i < n && @runnable
          begin
            ts = DateTime.now
            case @num_of_finish % 2
            when 0
              res = rc.set(i.to_s, 'hoge' + i.to_s)
              #puts "set k=#{i} #{res}" if res==nil || res.chomp != 'STORED'
              if res==nil || res.chomp != 'STORED'
                puts "set k=#{i} #{res}"
                @log.info "set k=#{i} #{res}"
              end
            when 1
              res = rc.get(i.to_s)
              #puts "get k=#{i} #{res}" if res == :error
              if res == :error
                puts "get k=#{i} #{res}"
                @log.info "get k=#{i} #{res}"
              end
            end
            t = (DateTime.now - ts).to_f * 86400.0
            @tmax = t if t > @tmax
            @tmin = t if t < @tmin
            @cnt += 1
            i += 1
          rescue => e
            p e
          end
        end
        @num_of_finish += 1
      rescue => e
        p e
      end
      private :sequential_reqs

    end

    class Scenario
      attr :working_path
      attr :roma_procs
      attr :stress
      attr :log

      def initialize(path, procs)
        @working_path = path
        @roma_procs = procs
        @stress = Stress.new 1
        @log = Logger.new "./test-scenario.log", "daily"
      end

      def init_roma
        @log.debug "begin init_roma"
        clean_file
        exec "bin/mkroute -d 7 #{RomaProc.to_str(@roma_procs)} --enabled_repeathost"
        @log.debug "end init_roma"
      end

      def clean_file
        @log.debug "begin clean_file"
        exec "rm -rf localhost_1121?*"
        @log.debug "end clean_file"
      end

      def clean_proc_file addr, port
        @log.debug "begin clean_tc"
        exec "rm -rf #{addr}_#{port}*"
        @log.debug "end clean_tc"
      end

      def start_roma
        @log.debug "begin start_roma"
        @roma_procs.length.times { |i|
          start_roma_proc i
        }
        @log.debug "end start_roma"
      end

      def start_roma_proc i
        @log.debug "begin start_roma_proc"
        str = "bin/romad #{@roma_procs[i].addr} -p #{@roma_procs[i].port.to_s} -d --enabled_repeathost"
        exec str
        @roma_procs[i].pid = get_pid(str)
        @log.debug "end start_roma_proc"
      end

      def join_roma_proc i, j
        @log.debug "begin join_roma_proc"
        str = "bin/romad #{@roma_procs[i].addr} -p #{@roma_procs[i].port.to_s} -j #{@roma_procs[j].addr}_#{@roma_procs[j].port.to_s} -d --enabled_repeathost"
        exec str
        @roma_procs[i].pid = get_pid(str)
        @log.debug "end start_roma_proc"
      end

      def exec cmd
        `cd #{@working_path}; #{cmd}`
      end

      def get_pid reg_str
        open("| ps -ef | grep romad") { |f|
          while l = f.gets
            return $1.to_i if l =~ /(\d+).+ruby\s#{reg_str}/
          end
        }
        nil
      end

      def stop_roma
        @log.debug "begin stop_roma"
        @roma_procs.length.times { |i|
          stop_roma_proc i
        }
        @log.debug "end stop_roma"
      end

      def stop_roma_proc i
        @log.debug "begin stop_roma_proc"
        begin
          exec "kill -9 #{@roma_procs[i].pid}"
        rescue e
          @log.error e
        end
        @log.debug "end stop_roma_proc"
      end

      def start_roma_client addr, port, req=nil
        @stress.start addr, port, req
      end

      def stop_roma_client
        @stress.runnable = false
      end

      def single_start_roma_client addr, port, n, req=nil
        @stress.single_start addr, port, n, req
      end

      def single_write_roma_client addr, port, n, req=nil
        @stress.num_of_finish = 0
        @stress.single_start addr, port, n, req
      end

      def single_read_roma_client addr, port, n, req=nil
        @stress.num_of_finish = 1
        @stress.single_start addr, port, n, req
      end

      def send_recover addr, port
        commander = Roma::MultiCommander.new "#{addr}_#{port}"
        res = commander.send_cmd "recover", "#{addr}_#{port}"
        puts res
      end

      def send_stats addr, port
        commander = Roma::MultiCommander.new "#{addr}_#{port}"
        res = commander.send_cmd "stats run", "#{addr}_#{port}"
        puts res
      end

      def send_stats_routing_nodes_length addr, port
        commander = Roma::MultiCommander.new "#{addr}_#{port}"
        res = commander.send_cmd "stats routing.nodes.length", "#{addr}_#{port}"
        splited = res.split(' ')
        splited.each_with_index { |w, i|
          if w == "routing.nodes.length"
            return splited[i + 1].to_i
          end
        }
        #raise "not found a specified property: routing.nodes.length"
        nil
      end
      
      def send_stats_run_acquire_vnodes addr, port
        commander = Roma::MultiCommander.new "#{addr}_#{port}"
        res = commander.send_cmd "stats stats.run_acquire_vnodes", "#{addr}_#{port}"
        splited = res.split(' ')
        splited.each_with_index { |w, i|
          if w == "stats.run_acquire_vnodes"
            return splited[i + 1] == "true"
          end
        }
        #raise "not found a specified property: stats.run_acquire_vnodes"
        nil
      end

      def send_stats_run_recover addr, port
        commander = Roma::MultiCommander.new "#{addr}_#{port}"
        res = commander.send_cmd "stats stats.run_recover", "#{addr}_#{port}"
        splited = res.split(' ')
        splited.each_with_index { |w, i|
          if w == "stats.run_recover"
            return splited[i + 1] == "true"
          end
        }
        #raise "not found a specified property: stats.run_recover"
        nil
      end
    end
  end
end
