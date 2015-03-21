rm(list = ls())
setwd("C:\\Users\\ambar_000\\Desktop\\597\\Amazon dataset")
rm(list = ls())

#amazon <- read.csv("Amazon_Instant_Video_no_text2.csv")
amazon_instant_video <- read.csv("Amazon_Instant_Video.csv")

str(amazon)
table(count.fields("Amazon_Instant_Video_no_text2.csv", sep=",", quote=""))
txt <-readLines("output.csv")[which(count.fields("output.csv", quote="", sep=",") == 19)]


load("amazonInstantVideo.rdata")
convertNAsToZero <- function(v) {
 ifelse(is.na(v),0,v)
}
ptm <- proc.time()
amazonNoNA = as.data.frame(apply(amazon,2,convertNAsToZero))
proc.time() - ptm
amazonSample <- t(amazonNoNA[sample(1:nrow(amazonNoNA), nrow(amazonNoNA)/10, replace=FALSE),])
calcDistForRowAndAllOthers <- function (row) {
 apply(amazonSample, 1, calcDistForTwoRows, row)
}
calcDistForTwoRows <- function(row1,row2) {
 intersectionEx1Ex2 <- sum(pmin(row1,row2))
 unionEx1Ex2 <- sum(row1) + sum(row2) - intersectionEx1Ex2

 intersectionEx1Ex2 / unionEx1Ex2
}
do <- apply (amazonSample,1, calcDistForRowAndAllOthers)
clusterTree <- hclust(as.dist(do),method="ward.D")
plot(clusterTree, labels = NULL, hang = 0.1, check = TRUE,
 axes = TRUE, frame.plot = FALSE, ann = TRUE,
 main = "Cluster Dendrogram",
 sub = NULL, xlab = NULL, ylab = "Height"); 
 


  amazon$product.productId = NULL
  
  
  sampled <- amazon[sample(nrow(amazon), 10000), ]
  d <- dist(sampled, method = "euclidean")
  clusters <- hclust(d, method = "ward.D")
  plot(clusters, lebels = NULL, hang = 0.1, check = TRUE, axes = TRUE, frame.plot = FALSE, ann = TRUE, main = "Cluster Dendrogram", sub = NULL, sxlab = NULL, ylab = "Height")
  groups <- cutree(clusters, k = 4)
  png("clusplot.png")
  clusplot(sampled, groups, color=TRUE, shade=TRUE, labels=2, lines=0)
  dev.off()
  
  
   pamCLust <- pam(sampled, 3, metric="euclidean")
     png("clusplot.png")
  clusplot(sampled, pamCLust$cluster, color=TRUE, shade=TRUE, labels=2, lines=0)
  dev.off()