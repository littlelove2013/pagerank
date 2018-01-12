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
		#if u !=v:
		#	self.node_n[v].append(u)
	#获取dict的关键字集合,即节点name
	def nodes(self):
		return self.node_n.keys()

def getGraph(data_file="data/simpletest"):
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

class GraphWithBlock():

	def __init__(self,data_file="data/WikiData.txt"):
		self.block_cap=2000#一个块的容量
		nodelist=[]
		self.Matric={}
		self.node_n={}
		dataFile = open(data_file)
		for line in dataFile:
			data = line.strip().split()
			if len(data) != 2:
				print('src data read error!')
			A = data[0]
			B = data[1]
			nodelist.append(A)
			nodelist.append(B)
			# 添加边倒节点，如果边的节点不存在，则添加该节点
			# g.add_edge((A, B))
		self.add_nodes(nodelist)
		print(self.block_cap)
		#获取分块的R
		self.initR()
		dataFile = open(data_file)
		i=0
		for line in dataFile:
			data = line.strip().split()
			if len(data) != 2:
				print('src data read error!')
			A = data[0]
			B = data[1]
			self.add_edge((A,B))
			if i%1000==0:
				print("%d/20000"%(i))
			i+=1
		dataFile.close()
	def add_nodes(self,nodelist):
		for i in range(len(nodelist)):
			node=nodelist[i]
			self.add_node(node)
	#对每一个节点,建一个链表(数组),链表(数组)保存的是其指向的节点
	def add_node(self,node):
		if not node in self.nodes():
			self.node_n[node]=[]

	def add_edge(self,edge):
		u,v=edge
		self.add_nodes(edge)#
		# blocknum=[k for k in range(len(self.R)) if v in self.R[k]][0]
		blocknum=self.node_n[v]
		if (v not in self.Matric[blocknum][u]):# and (u not in self.node_n[v]):#为什么要求u not in v呢?
			# self.node_n[u].append(v)#u->v
			self.Matric[blocknum][u].append(v)
			# print(u,'->',v)
		#if u !=v:
		#	self.node_n[v].append(u)
	def initR(self):
		# 对所有node分块
		nodes = list(self.nodes())
		lens = len(nodes)
		self.blocks = math.ceil(lens / self.block_cap)
		# 得到分段的R值
		self.R = [nodes[i-self.block_cap:i] for i in range(self.block_cap, lens+self.block_cap, self.block_cap)]
		nnodes={node:[] for node in nodes}
		for i in range(len(self.R)):
			self.Matric[i]=deepcopy(nnodes)
			print("lens:%d/%d"%(len(self.R),i))
			for k in self.R[i]:
				self.node_n[k]=i
		# self.Matric={k:deepcopy(nnodes) for k in range(len(self.R))}
	def getblockMatrix(self):

		#根据分段的R，对M做分块，把M分为只包含R中每段值的Matrix
		for i in range(blocks):
			#对每一个点
			Mblocks={}
			for node in nodes:
				tmp=[outdgree for outdgree in self.node_n[node] if outdgree in self.R[i]]
				# for outdgree in self.node_n[node]:
				# 	#对每个输出度,如果属于输出rnew的节点，则保留
				# 	if outdgree in self.R[i]:
				# 		tmp.append(outdgree)
				if len(tmp)>0:
					Mblocks[node]=tmp
			#对分块的矩阵做融合
			self.Matric[i]=Mblocks
		return self.R,self.Matric
	#获取dict的关键字集合,即节点name
	def nodes(self):
		return self.node_n.keys()

	def getRandM(self):
		return self.R,self.Matric



if __name__ == '__main__':
	# g=getGraph()
	# R,M=getRandM()
	g=GraphWithBlock()
	R,M=g.getRandM()
	# print('asdf')