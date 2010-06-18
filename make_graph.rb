require 'rubygems'
require 'wukong'

module MakeGraph
  class Mapper < Wukong::Streamer::LineStreamer
    def process line
      ids = line.scan(/\d+/)
      from_id = ids.shift
      ids.each {|to_id| yield(from_id,to_id)}
    end

  end
end

Wukong::Script.new(
  MakeGraph::Mapper,
  nil
  ).run # Execute the script
