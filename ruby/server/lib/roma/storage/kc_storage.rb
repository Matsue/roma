require 'kyotocabinet'
require 'roma/storage/basic_storage'

module Roma
  module Storage

    class KCStorage < BasicStorage
      include KyotoCabinet

      class KyotoCabinet::DB
        alias_method :rnum, :count

        alias_method :get_org, :get
        def get key
          ret = get_org key
          if ret == nil && self.error != KyotoCabinet::Error::NOREC
            raise StorageException, self.error
          end
          ret
        end
        
        def put key, value
          ret = set key, value
          raise StorageException, self.error unless ret
          ret
        end
        
        def out key
          ret = delete key
          if ret == false && self.error != KyotoCabinet::Error::NOREC
            raise StorageException, self.error
          end
          ret
        end
      end

      def initialize
        super
        @ext_name = 'kch'
      end

      def get_stat
        ret = super
        @hdb.each_with_index{ |hdb, idx|
          ret["storage[#{idx}].path"] = File.expand_path(hdb.path)
          ret["storage[#{idx}].count"] = hdb.count
          ret["storage[#{idx}].size"] = hdb.size
        }
        ret
      end

      def each_clean_up(t, vnhash)
        @do_clean_up = true
        nt = Time.now.to_i
        @hdb.each{ |hdb|
          keys = []
          hdb.each{ |k, v| keys.push(k) if v }
          keys.each{ |k|
            v = hdb[k]
            return unless @do_clean_up
            vn, last, clk, expt = unpack_header(v)
            vn_stat = vnhash[vn]
            if vn_stat == :primary && ( (expt != 0 && nt > expt) || (expt == 0 && t > last) )
              yield k, vn
              hdb.out(k) if hdb.get(k) == v
            elsif vn_stat == nil && t > last
              yield k, vn
              hdb.out(k) if hdb.get(k) == v
            end
            sleep @each_clean_up_sleep
          }
        }
      end

      def each_vn_dump(target_vn)
        count = 0
        @divnum.times{|i|
          tn =  Time.now.to_i
          keys = []
          @hdb[i].each{ |k, v| keys.push(k) if v }
          keys.each{|k|
            v = @hdb[i][k]
            vn, last, clk, expt, val = unpack_data(v)
            if vn != target_vn || (expt != 0 && tn > expt)
              count += 1              
              sleep @each_vn_dump_sleep if count % @each_vn_dump_sleep_count == 0
              next
            end
            if val
              yield [vn, last, clk, expt, k.length, k, val.length, val].pack("NNNNNa#{k.length}Na#{val.length}")
            else
              yield [vn, last, clk, expt, k.length, k, 0].pack("NNNNNa#{k.length}N")
            end
          }
        }
      end

      def each_hdb_dump(i,except_vnh = nil)
        count = 0
        keys = []
        @hdb[i].each{ |k, v| keys.push(k) if v }
        keys.each{|k|
          v = @hdb[i][k]
          vn, last, clk, expt, val = unpack_data(v)
          if except_vnh && except_vnh.key?(vn) || Time.now.to_i > expt
            count += 1
            sleep @each_vn_dump_sleep if count % @each_vn_dump_sleep_count == 0
          else
            yield [vn, last, clk, expt, k.length, k, val.length, val].pack("NNNNNa#{k.length}Na#{val.length}")
            sleep @each_vn_dump_sleep
          end
        }
      end

      # Create vnode dump.
      def get_vnode_hash(vn)
        buf = {}
        count = 0
        hdb = @hdb[@hdiv[vn]]
        keys = []
        hdb.each{ |k, v| keys.push(k) if v }
        keys.each{ |k|
          v = hdb[k]
          count += 1
          sleep @each_vn_dump_sleep if count % @each_vn_dump_sleep_count == 0
          dat = unpack_data(v) #v.unpack('NNNN')
          buf[k] = v if dat[0] == vn
        }
        return buf
      end
      private :get_vnode_hash

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
