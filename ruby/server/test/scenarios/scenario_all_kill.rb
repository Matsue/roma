require 'roma/tools/test-scenario'

module Roma
  module Test
    class Scenario_all_kill < Roma::Test::Scenario
      def initialize(s)
        test_all_kill(s)
      end

      def test_all_kill(s)
      s.log.info "begin scenario all_kill"
      s.roma_procs.count.times do |i|
	str = "bin/romad #{s.roma_procs[i].addr} -p #{s.roma_procs[i].port.to_s} -d --enabled_repeathost"
        s.roma_procs[i].pid = s.get_pid(str) if s.get_pid(str) != nil

        s.roma_procs.count.times do |j|
          str = "bin/romad #{s.roma_procs[i].addr} -p #{s.roma_procs[i].port.to_s} -j #{s.roma_procs[j].addr}_#{s.roma_procs[j].port.to_s} -d --enabled_repeathost"
          s.roma_procs[i].pid = s.get_pid(str) if s.get_pid(str) != nil
        end

        s.stop_roma_proc i if s.roma_procs[i].pid != nil
      end
      s.clean_file
      
      s.log.info "end scenario all_kill"
      end
    end #Scenario_all_kill
  end #Test
end #Roma
