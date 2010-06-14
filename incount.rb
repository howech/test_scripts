require 'rubygems'
require 'wukong'
class Array
 def sum
  x = 0
  each {|l| x+=l}
  return x
 end
end

module InCount
  class Mapper < Wukong::Streamer::LineStreamer
    attr_accessor :link_count
    def initialize *args
      self.link_count = {}
    end
    def stream *args
      super *args
      self.link_count.each do |key, count|
        emit [key, count].to_flat
      end
    end
    def process line
      words = line.strip.scan(/\[\[\s*([^\]]+?)\s*\]\]/) {|word| link_count[word] ||=0; link_count[word] += 1}
    end
  end

  #class Reducer < Wukong::Streamer::ListReducer
  #  def finalize
  #    yield [ key, values.map(&:last).map(&:to_i).sum ]
  #  end
  #end

  class GroupByReducer < Wukong::Streamer::AccumulatingReducer
      attr_accessor :sum

      # Start with an empty sum
      def start! *args
        self.sum = 0
      end

      # Accumulate value in turn
      def accumulate key, value
        self.sum += value.to_i
      end

      def finalize
        yield [key, sum]
      end
    end

    class Script < Wukong::Script
    # There's just the one field
    def default_options
      super.merge :sort_fields => 1, :reduce_tasks => 1
    end
  end
end

InCount::Script.new(
  InCount::Mapper,
  InCount::GroupByReducer
  ).run # Execute the script
