# -- coding: utf-8 --
# 
# PageRank工程的主程序
# 1. 数据导入
# 2. 数据处理和算法运算
# 3. 结果保存
#
import pagerank

if __name__ == '__main__':
    beta=0.85
    srcfile = "data/WikiData.txt"
    block_cap=2000
    # srcfile="data/simpletest"
    # block_cap=2
    pagerank.gcpagerank(beta,srcfile,block_cap)