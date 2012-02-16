require 'roma/tools/test-scenario'

module Roma
  module Test
    class Scenario_all_kill < Roma::Test::Scenario
      def initialize(s)
      s.log.info "begin scenario all_kill"

      s.roma_procs.length do |i|
        s.stop_roma_proc i
      end
      s.clean_file
      
      s.log.info "end scenario all_kill"
      end
    end #Scenario_all_kill
  end #Test
end #Roma
