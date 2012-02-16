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

        # stress
        s.start_roma_client s.roma_procs[0].addr, s.roma_procs[0].port, "sequential_reqs"
        sleep 3

        nlen = s.send_stats_routing_nodes_length s.roma_procs[0].addr, s.roma_procs[0].port
        if nlen != 3
          #raise "fatal error nlen: #{nlen}"
          s.log.warn "fatal error nlen: #{nlen}"
        end
        sleep 1

        100.times do
          # stop the specified roma process
          s.stop_roma_proc 2
          sleep 10

          nlen = s.send_stats_routing_nodes_length s.roma_procs[0].addr, s.roma_procs[0].port
          if nlen != 2
            #@raise "fatal error nlen: #{nlen}"
            s.log.warn "fatal error nlen: #{nlen}"
          end
          sleep 1

          s.join_roma_proc 2, 0
          sleep 10

          nlen = s.send_stats_routing_nodes_length s.roma_procs[0].addr, s.roma_procs[0].port
          if nlen != 3
            #raise "fatal error nlen: #{nlen}"
            s.log.warn "fatal error nlen: #{nlen}"
          end
          sleep 1
        end

        #stop
        s.stop_roma_client
        s.stop_roma_proc 0
        s.stop_roma_proc 1
        s.stop_roma_proc 2
        s.clean_file

        s.log.info "end scenario test_kill_join_recover"
      end
    end #Scenario_kill_join_recover
  end #Test
end #Roma
