# -- coding: utf-8 --
# 
# pagerank的核心算法程序
# 1. pagerank的计算公式
# 2. dead ends和splider trap处理
# 3. 矩阵分块化处理
# 

import graph
import time

def gcpagerank(beta=0.8,srcfile="data/WikiData.txt",block_cap=2000):
    thre=1e-6
    # srcfile="data/WikiData.txt"
    # block_cap=2000
    # srcfile="data/simpletest"
    # block_cap=2
    print("----读取源数据\n\t待读取数据集为：%s"%(srcfile))
    start = time.time()
    g=graph.getGraph(srcfile,block_cap)
    M,R,blocks,N=g.getblockMatrix()
    print('\t读取源数据为分块稀疏矩阵时间为：%fs\n'%(time.time()-start))
    print("----迭代求rank teleport parameter=%f"%(beta))
    #计时
    itime=0
    ttime=0
    #求和
    Rsum=0
    #迭代
    iter=0
    l1=1
    while(l1>thre):
        iter+=1#迭代次数
        l1=0
        newR = {}
        S=0
        start = time.time()
        S=0
        rS=0
        for i in range(blocks):
            m=M[i]#对于每一块
            r=R[i]
            newR[i]={node:0 for node in r.keys()}
            for line in m:
                src,blocknum,degree,outlink=line#因为抛弃了很多死节点，所以不存在degree==0的情况
                for node in outlink:
                    newR[i][node]+=beta*R[blocknum][src]/degree
            S += sum(newR[i].values())
            rS += sum(R[i].values())
        # print('new rank和%f,rank 和%f'%(S,rS))
        ttime+=time.time()-start
        # 对每个节点，再分配S/N的补偿
        tS=0
        p = (1 - S) / N
        for i in range(blocks):
            start = time.time()
            for node in newR[i].keys():
                newR[i][node] +=p
            # 计算R[i]与newR[i]的差值
            RR =[];NN =[];j=0;
            for j in R[i].keys():  
                RR.append(R[i][j])
                NN.append(newR[i][j])
            l1 += sum([abs(c - d) for c, d in zip(RR, NN)])
            ttime += time.time() - start
            tS+=sum(newR[i].values())
        #显示
        print('\t迭代次数：%d ,\trank总和：%f,\tS(new rank)=%f, \tl1=%e'%(iter,tS,S,l1))
        #将newR赋值给R
        R=newR.copy()
    itime=ttime/iter
    print('\t平均每次迭代时间：%f,总的迭代时间：%f\n'%(itime,ttime))
    print("----保存数据")
    top100dict = []
    #做完迭代，将权值写入文本
    print("The final page rank is saving...")
    filename="results/all_tp("+str(beta)+")_rankresult.txt"
    with open(filename, "w") as f_w:
	    for b in range(blocks):
		    keys=list(R[b].keys())
		    values = list(R[b].values())
		    top100dict.extend((sorted(R[b].items(),key=lambda x:x[1],reverse=True))[:100])#每次取前100个
		    for key, value in zip(keys,values):
			    f_w.write(key + '\t\t' + str(value) + '\n')
    f_w.close()
    print("The all page rank is save to %s"%(filename))
    # 取出排名前100的节点
    top100=(sorted(top100dict,key=lambda x:x[1],reverse=True))[:100]
    top100name="results/top100_tp("+str(beta)+")_rankresult.txt"
    with open(top100name, "w") as f_w:
	    for key, value in top100:
		    f_w.write(key + '\t\t' + str(value) + '\n')
    f_w.close()
    print("The top100 page rank is save to %s" % (filename))

if __name__ == '__main__':
    gcpagerank()