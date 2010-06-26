require 'rubygems'
require 'wukong'

module WPIDCrossOver
  class Mapper < Wukong::Streamer::LineStreamer
    def process line
      key,ida,idb = nil,nil,nil
      case line
         when /<title>(.*?)<\/title>\s*<id>\s*(\d+)\s*<\/id>/
	   # We are looking for different types of input lines. This one has the text of the complete 
           # wikipedia article as an xml document. We want to grab the title and the id.
           key,ida=$1,$2
           # We need to clean up title whitespace a little bit.
           key.gsub!(/\s+/,'_')
         when /^(\d+)\t(\S+)/
           # The other type of line just has an id and a title on it.
           idb,key=$1,$2
      end
      # Spit out all three fields (title, id from xml file, and id from titles file. The reduce phase will
      # pull these together.
      yield [key.to_s, ida.to_s, idb.to_s] if key;
    end
  end

  class SpliceReducer < Wukong::Streamer::AccumulatingReducer         
    attr_accessor :columns            
    # reset the columnts to empty
    def start! *args        
      self.columns = []
    end
    def recordize line
      line.split(/\t/) rescue nil
    end
    def accumulate *args
      args.each_index do |i|
        # accumulate fields where they are empty. There is an ordering issue here if you have 
        # overlapping columns. In this implementation, the first encountered non-empty field will win.
        self.columns[i] = args[i] if !columns[i] || columns[i].length==0
      end
    end
    def finalize   
      yield self.columns
    end
  end 
end

Wukong::Script.new(
  WPIDCrossOver::Mapper,
  WPIDCrossOver::SpliceReducer
  ).run # Execute the script
