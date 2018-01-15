# -- coding: utf-8 --
# 
# PageRank工程的Graph创建程序
#
from collections import deque
import math
from copy import deepcopy
class Graph():
	def __init__(self):
		self.node_n={}
	def add_nodes(self,nodelist):
		for i in nodelist:
			self.add_node(i)
	#对每一个节点,建一个链表(数组),链表(数组)保存的是其指向的节点
	def add_node(self,node):
		if not node in self.nodes():
			self.node_n[node]=[]

	def add_edge(self,edge):
		u,v=edge
		self.add_nodes(edge)#
		if (v not in self.node_n[u]):# and (u not in self.node_n[v]):#为什么要求u not in v呢?
			self.node_n[u].append(v)#u->v
	#获取dict的关键字集合,即节点name
	def nodes(self):
		return self.node_n.keys()
	def getblockMatrix(self):
		nodes=list(self.nodes())
		lens=len(nodes)
		self.block_cap=2000
		self.blocks = math.ceil(lens / self.block_cap)
		# 得到分段的R值,R保存的是[src,rank]
		# self.R = [nodes[i - self.block_cap:i] for i in range(self.block_cap, lens + self.block_cap, self.block_cap)]
		self.R={i:{} for i in range(self.blocks)}
		for i in range(lens):
			k=math.floor(i/self.block_cap)
			node=nodes[i]
			self.R[k][node]=1/lens#初始化rank分
		
		# print("S=",sum([sum(self.R[k].values()) for k in range(self.blocks)]))
		# self.blockdict={node:blocknum for node,blocknum in zip(nodes,range(0,lens))}
		self.Matric={i:[] for i in range(self.blocks)}
		# 根据分段的R，对M做分块，把M分为只包含R中每段值的Matrix
		for i in range(len(nodes)):
			# if (i+1)%1000==0:
			# 	print("i=%d" % (i+1))
			node = nodes[i]
			blocknum=math.floor(i/self.block_cap)
			degree=len(self.node_n[node])
			src=node
			destination = [outdgree for outdgree in self.node_n[node] if outdgree in self.R[blocknum].keys()]
			if len(destination) > 0:
				# tmp=[src,1.0 / self.block_cap,degree]
				tmp = [src, blocknum,degree]
				tmp.append(destination)
				self.Matric[blocknum].append(tmp)
		return self.Matric,self.R,self.blocks,lens
def getGraph(data_file="data/WikiData.txt"):
	g = Graph()
	dataFile = open(data_file)
	for line in dataFile:
		data=line.strip().split()
		if len(data)!=2:
			print('src data read error!')
		A = data[0]
		B = data[1]
		# self.List.append(A)
		# self.List.append(B)
		#添加边倒节点，如果边的节点不存在，则添加该节点
		g.add_edge((A,B))
	dataFile.close()
	return g

if __name__ == '__main__':
	g=getGraph()
	M,R,b,N=g.getblockMatrix()
	# print(b)
	# print(M[0])
