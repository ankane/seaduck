require "bundler/gem_tasks"
require "rake/testtask"

CATALOGS = %w(glue rest s3tables)

CATALOGS.each do |catalog|
  namespace :test do
    task("env:#{catalog}") { ENV["CATALOG"] = catalog }

    Rake::TestTask.new(catalog => "env:#{catalog}") do |t|
      t.description = "Run tests for #{catalog}"
      t.libs << "test"
      t.test_files = FileList["test/**/*_test.rb"]
    end
  end
end

task :test do
  Rake::Task["test:rest"].invoke
end

task default: :test
