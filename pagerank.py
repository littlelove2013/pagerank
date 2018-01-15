# -- coding: utf-8 --
# 
# pagerank的核心算法程序
# 1. pagerank的计算公式
# 2. dead ends和splider trap处理
# 3. MapReduce的矩阵分块化处理
# 

#
#MapReduce的Python
#
import itertools
import graph
import time
import numpy as np
from copy import deepcopy
class MapReduce:

    def map_reduce(i, mapper, reducer):
        """
        map_reduce方法
        :param i: 需要MapReduce的集合
        :param mapper: 自定义mapper方法
        :param reducer: 自定义reducer方法
        :return: 以自定义reducer方法的返回值为元素的一个列表
        """
        intermediate = []  # 存放所有的(intermediate_key, intermediate_value)
        for (key, value) in i.items():
            intermediate.extend(mapper(key, value))

        # print(intermediate)
        # sorted返回一个排序好的list，因为list中的元素是一个个的tuple，key设定按照tuple中第几个元素排序
        # groupby把迭代器中相邻的重复元素挑出来放在一起,key设定按照tuple中第几个元素为关键字来挑选重复元素
        # 下面的循环中groupby返回的key是intermediate_key，而group是个list，是1个或多个
        # 有着相同intermediate_key的(intermediate_key, intermediate_value)
        groups = {}
        for key, group in itertools.groupby(sorted(intermediate, key=lambda im: im[0]), key=lambda x: x[0]):
            groups[key] = [y for x, y in group]

        # groups是一个字典，其key为上面说到的intermediate_key，value为所有对应intermediate_key的intermediate_value
        # 组成的一个列表
        return [reducer(intermediate_key, groups[intermediate_key]) for intermediate_key in groups]

#
#pagerank的核心算法
#
# class pagerank:
#
#     def __init__(self, dg, block):
#         self.damping_factor = 0.8           # 阻尼系数,即α
#         self.max_iterations = 100            # 最大迭代次数
#         self.min_delta = 0.00001             # 确定迭代是否结束的参数,即ϵ
#         self.num_of_pages = len(dg.nodes())  # 总网页数
#
#         # graph表示整个网络图。是字典类型,表示矩阵M。
#         # graph[i][0] 存放第i网页的PR值，初始化为1/N，N为总的网页数
#         # graph[i][1] 存放第i网页的出链数量
#         # graph[i][2] 存放第i网页的出链网页，是一个列表
#         Matrix,SP,blocks = dg.getblockMatrix()
#         self.graph = {}
#         for i in SP[block]:
#             self.graph[i] = Matrix[block]
#
#         for node in dg.nodes():
#             self.graph[node] = [1.0 / self.num_of_pages, len(dg.node_n[node]), dg.node_n[node]]
#         # print(self.graph)
#     def ip_mapper(self, input_key, input_value):
#         """
#         看一个网页是否有出链，返回值中的 1 没有什么物理含义，只是为了在
#         map_reduce中的groups字典的key只有1，对应的value为所有的dead end
#         的PR值
#         :param input_key: 网页名，如 A
#         :param input_value: self.graph[input_key]
#         :return: 如果没有出链，即dead end，那么就返回[(1,这个网页的PR值)]；否则就返回[]
#         """
#         if input_value[1] == 0:
#             return [(1, input_value[0])]
#         else:
#             return []
#
#     def ip_reducer(self, input_key, input_value_list):
#         """
#         计算所有dead end的PR值之和
#         :param input_key: 根据ip_mapper的返回值来看，这个input_key就是:1
#         :param input_value_list: 所有dead end的PR值
#         :return: 所有dead end的PR值之和
#         """
#         return sum(input_value_list)
#
#     def S_mapper(self, input_key, input_value):
#         """
#         计算最终算法的S值，保证后面项的数值在迭代过程中和为1
#         """
#         return [(input_key,input_value[0])]
#
#
#     def S_reducer(self, input_key, input_value_list):
#
#         return self.damping_factor * sum(input_value_list)
#
#
#     def pr_mapper(self, input_key, input_value):
#         """
#         mapper方法
#         :param input_key: 网页名，如 A
#         :param input_value: self.graph[input_key]，即这个网页的相关信息
#         :return: [(网页名, 0.0), (出链网页1, 出链网页1分得的PR值), (出链网页2, 出链网页2分得的PR值)...]
#         """
#
#         return [(input_key, 0.0)] + [(out_link, input_value[0] / input_value[1]) for out_link in input_value[2]]
#
#
#     def pr_reducer_inter(self, intermediate_key, intermediate_value_list, dp, S):
#         """
#         reducer方法
#         :param intermediate_key: 网页名，如 A
#         :param intermediate_value_list: A所有分得的PR值的列表:[0.0,分得的PR值,分得的PR值...]
#         :param dp: 所有dead end的PR值之和
#         :return: (网页名，计算所得的PR值)
#         对应公式：
#         """
#         # print(intermediate_value_list)
#
#         return (intermediate_key,
#                 self.damping_factor * sum(intermediate_value_list) +
#                 self.damping_factor * dp / self.num_of_pages +
#                 (1.0 - S) / self.num_of_pages)
#
#     def page_rank(self):
#         """
#         计算PR值，每次迭代都需要两次调用MapReduce。一次是计算dead endPR值之和，一次
#         是计算所有网页的PR值
#         :return: self.graph，其中的PR值已经计算好
#         """
#         iteration = 1  # 迭代次数
#         change = 1  # 记录每轮迭代后的PR值变化情况，初始值为1保证至少有一次迭代
#         while change > self.min_delta:
#             print("Iteration: " + str(iteration))
#
#             # 因为可能存在dead end，所以才有下面这个dangling_list
#             # dangling_list存放的是[所有dead end的PR值之和]
#             # dp表示所有dead end的PR值之和
#             dangling_list = MapReduce.map_reduce(self.graph, self.ip_mapper, self.ip_reducer)
#             if dangling_list:
#                 dp = dangling_list[0]
#             else:
#                 dp = 0
#             S_valuelist = MapReduce.map_reduce(self.graph, self.S_mapper, self.S_reducer)
#             S_value = sum(S_valuelist)
#             # 因为MapReduce.map_reduce中要求的reducer只能有两个参数，而我们
#             # 需要传3个参数（多了一个所有dead end的PR值之和,即dp），所以采用
#             # 下面的lambda表达式来达到目的
#             # new_pr为一个列表，元素为:(网页名，计算所得的PR值)
#             new_pr = MapReduce.map_reduce(self.graph, self.pr_mapper, lambda x, y: self.pr_reducer_inter(x, y, dp, S_value))
#             # print(new_pr)
#             # 计算此轮PR值的变化情况
#             change = sum([abs(new_pr[i][1] - self.graph[new_pr[i][0]][0]) for i in range(self.num_of_pages)])
#             print("Change: " + str(change))
#
#             # 更新PR值
#             for i in range(self.num_of_pages):
#                 self.graph[new_pr[i][0]][0] = new_pr[i][1]
#             iteration += 1
#         return self.graph


def gcpagerank(beta=0.8):
    thre=1e-6
    srcfile="data/WikiData.txt"
    print("----读取源数据\n\t待读取数据集为：%s"%(srcfile))
    start = time.time()
    g=graph.getGraph(srcfile)
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
        # rS=0
        for i in range(blocks):
            m=M[i]#对于每一块
            r=R[i]
            newR[i]={node:0 for node in r.keys()}
            for line in m:
                src,blocknum,degree,outlink=line#因为抛弃了很多死节点，所以不存在degree==0的情况
                for node in outlink:
                    newR[i][node]+=R[blocknum][src]/degree
            S += sum(newR[i].values())
            # rS += sum(R[i].values())
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
            l1 += sum([abs(c - d) for c, d in zip(R[i].values(), newR[i].values())])
            ttime += time.time() - start
            tS+=sum(newR[i].values())
        #显示
        print('\t迭代次数：%d ,\trank总和：%f,\tS(new rank)=%f, \tl1=%e'%(iter,tS,S,l1))
        #将newR赋值给R
        R=newR
    itime=ttime/iter
    print('\t平均每次迭代时间：%f,总的迭代时间：%f\n'%(itime,ttime))
    print("----保存数据")
    k=[]
    v=[]
    #做完迭代，将权值写入文本
    print("The final page rank is saving...")
    filename="results/all_tp("+str(beta)+")_rankresult.txt"
    with open(filename, "w") as f_w:
	    for b in range(blocks):
		    keys=list(R[b].keys())
		    values = list(R[b].values())
		    k.extend(keys)
		    v.extend(values)
		    for key, value in zip(keys,values):
			    f_w.write(key + '\t\t' + str(value) + '\n')
    f_w.close()
    print("The all page rank is save to %s"%(filename))
    # 取出排名前100的节点
    k=np.array(k)
    v=np.array(v)
    index=(-v).argsort()
    top100keys=(k[index])[:100]
    top100values=(v[index])[:100]
    top100name="results/top100_tp("+str(beta)+")_rankresult.txt"
    with open(top100name, "w") as f_w:
	    for key, value in zip(top100keys, top100values):
		    f_w.write(key + '\t\t' + str(value) + '\n')
    f_w.close()
    print("The top100 page rank is save to %s" % (filename))
if __name__ == '__main__':
    gcpagerank()