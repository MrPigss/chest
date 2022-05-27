from heapq import heappop
from typing import List, Tuple

freelist: List[Tuple[int, int]] = []

def getFreeSpace(free_list:list[tuple[int,int]], space:int):
    heap = 0