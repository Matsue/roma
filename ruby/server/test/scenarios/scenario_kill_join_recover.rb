require 'roma/tools/test-scenario'

module Roma
  module Test
    class Scenario_kill_join_recover < Roma::Test::Scenario
      def initialize(s)
        test_kill_join_recover(s)
      end

      def test_kill_join_recover(s)
        s.log.info "begin scenario test_kill_join_recover"

        # initialize a ROMA
        s.init_roma

        # start a ROMA
        s.start_roma
        sleep 10
        20.times do
          nlen = s.send_stats_routing_nodes_length s.roma_procs[0].addr, s.roma_procs[0].port
          break if nlen == 3
          s.log.warn "fatal error nlen: #{nlen}"
          sleep 2
        end

        # data store
        s.single_write_roma_client s.roma_procs[0].addr, s.roma_procs[0].port, 1000, "sequential_reqs"
        sleep 3

        100.times do
          # data_check
          s.single_read_roma_client s.roma_procs[0].addr, s.roma_procs[0].port, 1000, "sequential_reqs"
          sleep 3

          # stop the specified roma process
          s.stop_roma_proc 2
          sleep 5
          s.clean_proc_file s.roma_procs[2].addr, s.roma_procs[2].port
          sleep 10 
          20.times do
            nlen = s.send_stats_routing_nodes_length s.roma_procs[0].addr, s.roma_procs[0].port
            break if nlen == 2
            s.log.warn "fatal error nlen: #{nlen}"
            sleep 2
          end

          #recover
          s.send_recover s.roma_procs[0].addr, s.roma_procs[0].port
          sleep 3
          while s.send_stats_run_recover(s.roma_procs[0].addr, s.roma_procs[0].port) || s.send_stats_run_recover(s.roma_procs[1].addr, s.roma_procs[1].port)
            s.log.info "waiting for finish of recover."
            sleep 5
          end

          #join
          s.join_roma_proc 2, 0
          sleep 20
          while s.send_stats_run_acquire_vnodes(s.roma_procs[0].addr, s.roma_procs[0].port) || s.send_stats_run_acquire_vnodes(s.roma_procs[1].addr, s.roma_procs[1].port) || s.send_stats_run_acquire_vnodes(s.roma_procs[2].addr, s.roma_procs[2].port)
            s.log.info "waiting for finish of join."
            sleep 5
          end
          20.times do
            nlen = s.send_stats_routing_nodes_length s.roma_procs[0].addr, s.roma_procs[0].port
            break if nlen == 3
            s.log.warn "fatal error nlen: #{nlen}"
            sleep 2
          end
        end

        #stop
        #s.stop_roma_client
        s.stop_roma_proc 0
        s.stop_roma_proc 1
        s.stop_roma_proc 2

        s.log.info "end scenario test_kill_join_recover"
      end
    end #Scenario_kill_join_recover
  end #Test
end #Roma
