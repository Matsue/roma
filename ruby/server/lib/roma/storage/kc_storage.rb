require 'kyotocabinet'
require 'roma/storage/basic_storage'

module Roma
  module Storage

    class KCStorage < BasicStorage
      include KyotoCabinet

      def initialize
        super
        @ext_name = 'kch'
      end

      def get_stat
        ret = super
        @hdb.each_with_index{ |hdb, idx|
          ret["storage[#{idx}].path"] = File.expand_path(hdb.path)
          ret["storage[#{idx}].rnum"] = hdb.count
          ret["storage[#{idx}].fsiz"] = hdb.size
        }
        ret
      end

      protected

      def validate_options(hdb)
        prop = parse_options
        prop.each_key{ |key|
          unless /^(apow|fpow|opts|bnum|msiz|dfunit|erstrm|ervbs)$/ =~ key
            raise RuntimeError.new("Syntax error, unexpected option #{key}")
          end
        }
      end

      private

      def parse_options
        return Hash.new(-1) unless @option
        buf = @option.split('#')
        prop = Hash.new(-1)
        buf.each{|equ|
          if /(\S+)\s*=\s*(\S+)/ =~ equ
            prop[$1] = $2
          else
            raise RuntimeError.new("Option string parse error.")
          end
        }
        prop
      end

      def open_db(fname)
        DB.class_eval do
          alias_method :put, :set
        end
        hdb = DB::new

        validate_options(hdb)

        unless hdb.open("#{fname}\##{@option}", DB::OWRITER | DB::OCREATE | DB::ONOLOCK)
          raise RuntimeError.new("kcdb open error: #{hdb.error}")
        end
        hdb
      end

      def close_db(hdb)
        unless hdb.close
          raise RuntimeError.new("kcdb close error: #{hdb.error}")
        end        
      end

    end # class KCStorage

  end # module Storage
end # module Roma
