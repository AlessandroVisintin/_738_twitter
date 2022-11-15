import math
import matplotlib.pyplot as plt


R = 0.05

OUT = []
for e in range(5,6):
	print(e)
	
	t = 10**e
	f = int(t * R)
	g = t - f
	s = 5000 if t * 0.05 > 5000 else int(t * 0.05)
	
	G = math.factorial(g)
	F = math.factorial(f)
	GF = math.factorial(g+f)
	

	OUT.append({})
	for i in range(0,101,1):
		print(i, end=' ')
		sf = int(s * (i / 100))
		sg = s - sf
		
		if sf in OUT[-1]:
			continue
		
		sG = G // (math.factorial(sg) * math.factorial(g-sg))
		sF = F // (math.factorial(sf) * math.factorial(f-sf))
		sGF = GF // (math.factorial(sg+sf) * math.factorial(g+f-sg-sf))
		
		OUT[-1][sf] = (i, sG * sF / sGF)
	print('')


for d in OUT:
	l = [v for k,v in d.items()]
	X,Y = list(zip(*l))
	print(sum(Y), X[max(range(len(Y)), key=Y.__getitem__)])
	plt.plot(X,Y)
plt.show()
