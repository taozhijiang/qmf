#include <fstream>
#include <iomanip>
#include <random>

// 用来产生-0.01 ~ 0.01 的均匀分布数据，保证wals计算的可重复性

int main(int argc, char** argv) {
  
  int count = 1000000;
  const std::string outfile="uniform.dat";

  if(argc > 1)
    count = std::atoi(argv[1]);

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_real_distribution<double> distr(-0.01, 0.01);

  std::ofstream fout(outfile);
  fout << std::fixed;
  fout << std::setprecision(9);

  for(int i=0; i<count; ++i) {
    auto dat = distr(gen);
    fout << dat << std::endl;
  }


  return 0;
}
