m = readmatrix('dict.csv')
edges_A = linspace(0,300,301)
edges = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30]
hist(m,edges_A)
xlim([0 200])
title("Histogram of number of tracking points per tropical storm")
ylabel("No. of storms")
xlabel("Tracking points")