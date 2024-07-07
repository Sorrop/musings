(ns musings.bheap.protocol)

(defprotocol BHeapProtocol
  (insert [heap element]
    "Inserts element to heap")

  (extract [heap]
    "Extracts the root of the heap")

  (delete [heap element]
    "Delete arbitrary element from heap")

  (update-elem [heap element updater]
    "Update the specified element in the heap based on the updater fn
     and maintain heap property")

  (size [heap]
    "Returns the size of the underlying ArrayList")

  (to-vec [heap]
    "Returns the backing array as a vector")

  (has? [heap element]
    "Returns true if heap contains element")

  (clear [heap]
    "Clears the heap"))
