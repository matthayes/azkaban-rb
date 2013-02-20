# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "azkaban-rb/version"

Gem::Specification.new do |s|
  s.name        = "azkaban-rb"
  s.version     = Azkaban::Rb::VERSION
  s.authors     = ["Matt Hayes","William Vaughan"]
  s.email       = ["matthew.terence.hayes@gmail.com"]
  s.homepage    = "https://github.com/matthayes/azkaban-rb"
  s.summary     = %q{Azkaban job generation using Ruby}
  s.description = %q{azkaban-rb allows Azkaban jobs to be modeled as rake tasks}

  s.rubyforge_project = "azkaban-rb"

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]
  
  s.add_dependency "httpclient", "~> 2.1.6"
  s.add_dependency "GraphvizR", "~> 0.5.1"
  s.add_dependency "rest-client", "~> 1.6.7"
end
