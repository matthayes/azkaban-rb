require 'graphviz_r'

class RakeGraph
  @@icon_base = '/Users/wvaughan/Workspaces/azkaban-rb/lib/icons'
  attr_reader :graph, :tasks, :jobs

  def initialize
    @tasks = {}
    Rake.application.tasks.find_all{ |task| not task.job.nil?}.each do |task|
      tasks[RakeGraph.task_name(task)] = task
    end
    @nodes = {}
    @edges = []
    construct_graph()
  end
  
  def RakeGraph.task_name(task)
    task_name = "#{task}"
    task_name = "#{task}".gsub(@@job_namespace+":", '') unless @@job_namespace.nil?
    return task_name
  end
  
  def RakeGraph.data_name(name)
    name = name.gsub('/', '_')
    name, ext = name.partition('.')
    return name
  end
  
  def construct_graph()
    @tasks.each do |task_name, task|
      node = TaskNode.new(task)
      @nodes[node.name] = node
      #find all prereq tasks
      task.prerequisites.each do |prereq|
        @edges << TaskEdge.new(prereq, node.name)
      end
      # find all data reads
      task.job.read_locks.each do |read_lock|
        data_name = RakeGraph.data_name(read_lock)
        @nodes[data_name] = DataNode.new(read_lock) unless @nodes.has_key? data_name
        @edges << DataEdge.new(data_name, node.name)
      end
      # find all data writes
      task.job.write_locks.each do |write_lock|
        data_name = RakeGraph.data_name(write_lock)
        @nodes[data_name] = DataNode.new(write_lock) unless @nodes.has_key? data_name
        @edges << DataEdge.new(node.name, data_name)
      end
    end    
  end
  
  class Node
    attr_reader :name, :type
    
    def initialize(name, type)
      @name = name
      @type = type
    end
   
    def label
      return "#{@name}".to_sym
    end
  
    def to_s
      return "#{@type}: #{@name}"
    end

  end
  
  class TaskNode < Node
    attr_reader :task
    
    def initialize(task)
      super(RakeGraph.task_name(task), task.job.class.to_s)
      @task = task
    end
    
    def label
      label = "<<table border='0' cellpadding='0' cellwidth='0'>
        <tr><td>#{@name}</td></tr>
        <tr><td>#{@type}</td></tr>"
      label += "<tr><td>#{@task.job.uses_arg}</td></tr>" unless @type == 'Azkaban::CommandJob'
      label += "</table>>"
      return label.to_sym
    end
    
    
    def shape
      return :ellipse
    end
  end
  
  class DataNode < Node
    def initialize(filename)
      super(RakeGraph.data_name(filename), "data")
      @filename = filename
    end
    
    def label
      name = @filename.gsub(@@hdfs_root+"/", '') unless @@hdfs_root.nil?
      return "<#{name}>".to_sym
    end
    
    def shape
      return :box
    end
  end
  
  class Edge
    attr_reader :source, :dest, :type
    
    def initialize(source, dest)
      @source = source
      @dest = dest
    end
    
    def to_s
      return "#{source} >> #{dest}"
    end
  end
  
  class TaskEdge < Edge
    def initialize(source, dest)
      super(source, dest)
      @type = "task"
    end
  end
  
  class DataEdge < Edge
    def initialize(source, dest)
      super(source, dest)
      @type = "data"
    end
  end
  
  def visualize(name, output_file)
    g = GraphvizR.new name
    g.graph[:label => name]
    add_nodes(g)
    add_edges(g)
    g.output output_file
  end
  
  def add_nodes(g)
    @nodes.each do |name, node|
      g[name] [:label => node.label, :shape => node.shape]
    end
  end
  
  def add_edges(g)
    @edges.each do |edge|
      g[edge.source]>>g[edge.dest]
    end
  end
end
