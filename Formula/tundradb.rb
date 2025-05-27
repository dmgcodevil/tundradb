class Tundradb < Formula
  desc "Graph database with SQL-like query language"
  homepage "https://github.com/yourusername/tundradb"
  url "https://github.com/yourusername/tundradb/archive/v1.0.0.tar.gz"
  sha256 "YOUR_SHA256_HERE"  # Update this with actual SHA256
  license "MIT"  # Update with your actual license

  depends_on "cmake" => :build
  depends_on "antlr4-cpp-runtime"
  depends_on "apache-arrow"
  depends_on "boost"
  depends_on "tbb"
  depends_on "libcds"
  depends_on "nlohmann-json"
  depends_on "spdlog"
  depends_on "openjdk@11" => :build

  def install
    # Set up build environment
    ENV["JAVA_HOME"] = Formula["openjdk@11"].opt_prefix

    # Create build directory
    mkdir "build" do
      system "cmake", "..", 
             "-DCMAKE_BUILD_TYPE=Release",
             "-DCMAKE_CXX_STANDARD=23",
             "-DCMAKE_CXX_STANDARD_REQUIRED=ON",
             *std_cmake_args
      system "make", "-j#{ENV.make_jobs}"
      
      # Install binaries
      bin.install "tundra_shell"
      bin.install "tundradb"
    end

    # Install documentation
    doc.install "README.md"
    
    # Create example directory
    (share/"tundradb").install "examples" if Dir.exist?("examples")
  end

  test do
    # Test that the binary runs and shows help
    assert_match "TundraDB", shell_output("#{bin}/tundra_shell --help")
  end
end 