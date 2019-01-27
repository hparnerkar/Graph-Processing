A directed graph is represented in the input text file using one line per graph vertex. For example, the line

1,2,3,4,5,6,7
represents the vertex with ID 1, which is linked to the vertices with IDs 2, 3, 4, 5, 6, and 7. Your task is to write a Map-Reduce program that partitions a graph into K clusters using multi-source BFS (breadth-first search). It selects K random graph vertices, called centroids, and then, at the first itearation, for each centroid, it assigns the centroid id to its unassigned neighbors. Then, at the second iteration. it assigns the centroid id to the unassigned neighbors of the neighbors, etc, in a breadth-first search fashion. After few repetitions, each vertex will be assigned to the centroid that needs the smallest number of hops to reach the vertex (the closest centroid). First you need a class to represent a vertex:
class Vertex {
  long id;          // the vertex ID
  Vector adjacent;  // the vertex neighbors
  long centroid;    // the id of the centroid in which this vertex belongs to
  short depth;      // the BFS depth
  ...
}

THe Psuedo-code:
Vertex has a constructor Vertex(id,adjacent,centroid,depth).
You need to write 3 Map-Reduce tasks. The first Map-Reduce job is to read the graph:

map ( key, line ) =
  parse the line to get the vertex id and the adjacent vector
  // take the first 10 vertices of each split to be the centroids
  for the first 10 vertices, centroid = id; for all the others, centroid = -1
  emit( id, new Vertex(id,adjacent,centroid,0) )
The second Map-Reduce job is to do BFS:
map ( key, vertex ) =
  emit( vertex.id, vertex )   // pass the graph topology
  if (vertex.centroid > 0)
     for n in vertex.adjacent:     // send the centroid to the adjacent vertices
        emit( n, new Vertex(n,[],vertex.centroid,BFS_depth) )

reduce ( id, values ) =
  min_depth = 1000
  m = new Vertex(id,[],-1,0)
  for v in values:
     if (v.adjacent is not empty)
        m.adjacent = v.adjacent
     if (v.centroid > 0 && v.depth < min_depth)
        min_depth = v.depth
        m.centroid = v.centroid
  m.depth = min_depth
  emit( id, m )
The final Map-Reduce job is to calculate the cluster sizes:
map ( id, value ) =
   emit(value.centroid,1)

reduce ( centroid, values ) =
   m = 0
   for v in values:
       m = m+v
   emit(centroid,m)
