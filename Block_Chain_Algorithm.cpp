#include<iostream>
#include<stdlib.h>
//concurrency
#include<thread>
// locking resource
#include<mutex>
#include<condition_variable>
//random number and time
#include<ctime>
#include<string>

std :: mutex coutMutex;



//utility function to print time in specific format
std::string printCurrentTime() {
  std::time_t sysTime;
  sysTime = std::time(nullptr);
  return("\n [ " + std::string(std::asctime(std::localtime(&sysTime))).substr(11,8) + " ] ||  ");
}

void locked_cout(std::string msg) {
  std::lock_guard<std::mutex> guard(coutMutex);
  std::cout <<msg;
}

// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
//Struct: Buffer - nodes in the bufferCache structure

struct Buffer{

	int blockNum;
  int serialNum;
  int deviceNum;
  bool isBusy;                                                                                                                                        //true = buys , false = free
  std :: mutex bufferMutex;                                                                                                                           // mutex  - used to lock current buffer
  std :: condition_variable bufferCondVar;                                                                                                            // condition variable - used for wait and notify_all
  int marked;                                                                                                                                         // whether buffer is marked for delayed-write
	Buffer *hashQueueNext,*hashQueuePrev,*freeListPrev,*freeListNext;                                                                                   //pointers for hash list and free list

	Buffer(int blockNum, int serialNum, int deviceNum = 0 , bool isBusy = false,Buffer *hashQueueNext =0,Buffer *hashQueuePrev=0,
    Buffer *freeListNext=0, Buffer *freeListPrev = 0,int marked = 0) {
    this->blockNum = blockNum;
    this->serialNum = serialNum;
    this->deviceNum = deviceNum;
    this->isBusy = isBusy;
	  this->hashQueueNext = hashQueueNext;
	  this->hashQueuePrev = hashQueuePrev;
    this->freeListNext = freeListNext;
    this->freeListPrev = freeListPrev;
    this->marked  = marked;
	}
};

//++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
//class CDLL - Circular Doubly Linked List: Each hashQueue and the freeList in BufferCache are maintained as one

class CDLL {
    public:
        Buffer *head,*tail;
        bool isEmpty;
        int sizeOfFreeList;                                                                                                                           // named so because only used in case of free list
        CDLL() {
            head=0;
            tail=0;
            isEmpty = true;
            sizeOfFreeList = 0;
        }
        void attachToTailInFreeList(Buffer*);
        void attachToHeadInFreeList(Buffer*);
        void attachToTailInHashList(Buffer*);
        Buffer* searchInHashList(int,int);
        void detachFromFreeList(Buffer*);
        void detachFromHashList(Buffer*);
};

//=============CDLL- function definitions=============================================================================================================

void CDLL::attachToTailInFreeList(Buffer *bf) {
	if(tail == 0) {
		tail = head = bf;
		tail->freeListNext = tail;
		tail->freeListPrev = tail;
	}
	else {
    tail->freeListNext = bf;
    bf->freeListPrev = tail;
    bf->freeListNext = head;
    head->freeListPrev = bf;
    tail = bf;
	}

  sizeOfFreeList++;
  isEmpty = false;
}

void CDLL::attachToTailInHashList(Buffer *bf) {
  if(tail == 0) {
		tail = head = bf;
		tail->hashQueueNext = tail;
		tail->hashQueuePrev = tail;
	}
	else {
    tail->hashQueueNext = bf;
    bf->hashQueuePrev = tail;
    bf->hashQueueNext = head;
    head->hashQueuePrev = bf;
    tail = bf;
	}
}

void CDLL::detachFromFreeList(Buffer* bf) {
  if(bf == head && head == tail) {
      head = tail =0;
  }
  else {
    if(bf == head) {
      tail->freeListNext = head->freeListNext;
      head->freeListNext->freeListPrev=head->freeListPrev;
      head = head->freeListNext;
    }
    else if(bf == tail) {
      tail->freeListPrev->freeListNext = head;
      head->freeListPrev = tail->freeListPrev;
      tail=tail->freeListPrev;
    }
    else {
      bf->freeListPrev->freeListNext = bf->freeListNext;
      bf -> freeListNext->freeListPrev= bf->freeListPrev;
    }
  }

  sizeOfFreeList--;
  if(sizeOfFreeList == 0 ) {
    isEmpty = true;
  }
}

void CDLL::detachFromHashList(Buffer* bf) {
  if(bf == head && head == tail) {
      head = tail =0;
  }
  else {
    if(bf == head) {
      tail->hashQueueNext = head->hashQueuePrev;
      head->hashQueueNext->hashQueuePrev=head->hashQueuePrev;
      head = head->hashQueueNext;
    }
    else if(bf == tail) {
      tail->hashQueuePrev->hashQueueNext = head;
      head->hashQueuePrev = tail->hashQueuePrev;
      tail=tail->hashQueuePrev;
    }
    else {
      bf->hashQueuePrev->hashQueueNext = bf->hashQueueNext;
      bf -> hashQueueNext->hashQueuePrev= bf->hashQueuePrev;
    }
  }
}

Buffer* CDLL::searchInHashList(int processBlockNum,int processDevNum) {
  // std::cout<<"\n searching in hashQUeue";
  Buffer *curr = head;
  if(head && curr->blockNum == processBlockNum && curr->deviceNum == processDevNum)
    return curr;
  else curr = curr->hashQueueNext;

  while(curr != head) {
    if( curr->blockNum == processBlockNum && curr->deviceNum == processDevNum )
      return curr;
    else
      curr = curr->hashQueueNext;
  }

  return 0;
}

void CDLL::attachToHeadInFreeList(Buffer* bf) {
	if(head)
    {
        bf->freeListNext = head;
        head->freeListPrev = bf;
		    tail->freeListNext = bf;
        bf->freeListPrev = tail;
        head = bf;
	}
    else
    {
        tail = head = bf;
		      tail->freeListNext = tail;
		tail->freeListPrev = tail;
    }

    isEmpty=false;
    sizeOfFreeList++;
}

// +++++++++++class: Process+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

class Process {
    public:
        int processBlockNum;
        int processDevNum;
        char processName;

      Process(int processBlockNum,int processDevNum,char processName) {
          this->processBlockNum = processBlockNum;
          this->processDevNum = processDevNum;
          this->processName = processName;
      }
};

// +++++++++++class: BufferCache - structure that contains Buffer nodes +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

class BufferCache {
  private:
    int numBuffersInOneQueue;
    int numHashQueues;
    CDLL **hashQueuesArray;
    CDLL *freeList;
    std :: mutex freeListMutex;                                                                                                                       //mutex to lock the buffer on the state of free list
    std :: condition_variable freeListCondVar;
    void createHashQueues();
    int hash(int,int);
    void writeToDisk(Buffer*);

  public:
    BufferCache(int numBuffers, int numHashQueues) {
      this->numBuffersInOneQueue = numBuffers/numHashQueues;
      this->numHashQueues = numHashQueues;
      createHashQueues();
      print();
    }
    Buffer* search(int,int);
    bool acquire(Buffer*, Process*);
    void print();
    void printhash(CDLL*);
    void printfree(CDLL*);

};

void BufferCache::printhash(CDLL* hl)
{
  Buffer* ptr = hl->head;
  std::string line1 = "(" + std::to_string(ptr->deviceNum) + "-" + std::to_string(ptr->blockNum) + ")" + "-->";
  locked_cout(line1);
  ptr = ptr->hashQueueNext;
  while(ptr != hl->head)
  {
    std::string line1 = "(" + std::to_string(ptr->deviceNum) + "-" + std::to_string(ptr->blockNum) + ")" + "-->";
    locked_cout(line1);
      ptr = ptr->hashQueueNext;
  }
  locked_cout("circularList");
}

void BufferCache::printfree(CDLL* hl)
{
  Buffer* ptr = hl->head;
  std::string line0 = "(" + std::to_string(ptr->deviceNum) + "-" + std::to_string(ptr->blockNum) + ")" + "-->";
  locked_cout(line0);
  ptr = ptr->freeListNext;
  while(ptr != hl->head)
  {
    std::string line1 = "(" + std::to_string(ptr->deviceNum) + "-" + std::to_string(ptr->blockNum) + ")" + "-->";
    locked_cout(line1);
      ptr = ptr->freeListNext;
  }
  locked_cout("circularList");
}
void BufferCache::print()
{
    // hashlist
    locked_cout("\n===================================================\n\n\tHASHLISTS :: device Number and Block Number\n\n");
    for(int i=0; i<numHashQueues; i++)
    {
      std::string line0 = "Hash" + std::to_string(i) + "\t\t";
      locked_cout(line0);
      printhash(hashQueuesArray[i]);
      locked_cout("\n");
    }

    locked_cout("\n\tFREELIST\n");
    // freelist
    printfree(freeList);
    locked_cout("\n===================================================\n");
}
// ================== BufferCache function definitions ===============================================================================================

//----------------- creates HashQueus in BufferCache-------------------------------------------------------------------------------------------------
void BufferCache::createHashQueues() {
    int counter = 0;
    hashQueuesArray = new CDLL*[numHashQueues];
    freeList =  new CDLL();
    for(int i = 0; i < numHashQueues; i++ ) {
      hashQueuesArray[i] = new CDLL();
      for(int j = 0; j < numBuffersInOneQueue; j++) {
        Buffer *bf =  new Buffer(counter,counter);
        hashQueuesArray[i]->attachToTailInHashList(bf);
        freeList->attachToTailInFreeList(bf) ;
        counter++;
      }
    }
  }

//-------------- hash function to hash buffers to lists on their device and block number--------------------------------------------------------------

int BufferCache::hash(int a , int b ) {
    return ((a*b) % numHashQueues);
}

//------------------- searches for buffer in the bufferCache
//if not found in hashLists sends freeList's head. If freeList is empty sends 0
Buffer* BufferCache::search(int processBlockNum, int processDevNum) {
    int hashedVal = hash(processBlockNum,processDevNum);
    std::string line0 = " for block : " + std::to_string(hashedVal);
    locked_cout(line0);
    Buffer* buffer = hashQueuesArray[hashedVal]->searchInHashList(processBlockNum,processDevNum);
    if(!buffer) {
      if(!freeList->isEmpty) {
        buffer = freeList->head;
        freeList->detachFromFreeList(buffer);
      }
    }
    return buffer;
}

//--------------------called when a buffer is marked dealyed-write-----------------------------------------------------------------------------------

void BufferCache::writeToDisk(Buffer* buffer) {

  if(freeList->isEmpty) {
    std::unique_lock<std::mutex> lock2(freeListMutex);

    std::string time = printCurrentTime();
    std::string line0 = time + "writing contents of buffer : " + std::to_string(buffer->serialNum);
    locked_cout(line0);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    time = printCurrentTime();
    std::string line1 = time + "contents of buffer : " + std::to_string(buffer->serialNum) + " written to disk. Adding to freeList's head.";
    locked_cout(line1);
    buffer->marked = 0; // delayed-write = false;
    freeList->attachToHeadInFreeList(buffer);

    freeListCondVar.notify_all();
    lock2.unlock();

  }
  else {
    std::string time = printCurrentTime();
    std::string line0 = time + "writing contents of buffer : " + std::to_string(buffer->serialNum);
    locked_cout(line0);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    time = printCurrentTime();
    std::string line1 = time + "contents of buffer : " + std::to_string(buffer->serialNum) + " written to disk. Adding to freeList's head."; // delayed-write = false;
    locked_cout(line1);
    freeList->attachToHeadInFreeList(buffer);
  }

}

//------------------------called when a buffer has been searched for in the bufferCache---------------------------------------------------------------

bool BufferCache :: acquire(Buffer* bufferToAcquire, Process* P) {
  std::string currentTime;
  //print();
  std::string line0;
  std::string line1;
  std::string times;
  if(bufferToAcquire) {                                                                                                                             // 0 is sent only if buffer wasn't found and free list was empty, if a buffer has been returned we check whther it was from a freeList or only a hshQueue
    if(bufferToAcquire->blockNum == P->processBlockNum && bufferToAcquire->deviceNum == P->processDevNum) {                                         // buffer was found buffer in hashList
      std::unique_lock<std::mutex> lock(bufferToAcquire->bufferMutex);                                                                              // lock is acquired on buffer
      if(!bufferToAcquire->isBusy) {                                                                                                                // if free, we check if it was marked delayed-write. Yes - start a thread to write to disk asynchronously.Search again. No - acquire
        if(bufferToAcquire->marked == 1) {
          times = printCurrentTime();
          line0 = times + "Process : "+ std::to_string(P->processName) + " found buffer in hashQueue, free but marked delayed-write. Starting asynchronous write from buffer : " + std::to_string(bufferToAcquire->serialNum);
          locked_cout(line0);
          bufferToAcquire->marked = 0;
          std::thread t(&BufferCache::writeToDisk,this,bufferToAcquire);                                                                            // this will begin concurrently
          t.detach();
          times = printCurrentTime();
          line1 = times + "Process : " + std::to_string(P->processName) + " starts search again ";
          locked_cout(line1);
          return true;
        }
        else {
          times = printCurrentTime();
          line0 = times + "Process : " + std::to_string(P->processName) + " found buffer : " + std::to_string(bufferToAcquire->serialNum) + " free. Acquiring. Removing from free list.";
          locked_cout(line0);
          if(freeList->isEmpty) {                                                                                                                   //if true acquire lock on freeList
            std::unique_lock<std::mutex> lock2(freeListMutex);
            bufferToAcquire->isBusy = true;
            times = printCurrentTime();                                                                                                       // marked busy
            line1 = times + "Process : " + std::to_string(P->processName) + " working on  buffer : " + std::to_string(bufferToAcquire->serialNum);
            locked_cout(line1);
            std::this_thread::sleep_for(std::chrono::seconds(2));                                                                                   // thread goes to sleep for 2 secs. Working.

            times = printCurrentTime();
            line1 = times + "Process : " + std::to_string(P->processName) + " releases buffer : " + std::to_string(bufferToAcquire->serialNum);
            locked_cout(line1);
            bufferToAcquire->isBusy = false;
            freeList->attachToTailInFreeList(bufferToAcquire);
            print();
            freeListCondVar.notify_all();
            lock2.unlock();
            bufferToAcquire->bufferCondVar.notify_all();                                                                                            // wake up all proesses sleeping for this buffer
            lock.unlock();
          }
          else {
            bufferToAcquire->isBusy = true;
            times = printCurrentTime();                                                                                                      // marked busy
            line0 = times + "Process : " + std::to_string(P->processName) + " working on  buffer : " + std::to_string(bufferToAcquire->serialNum);
            locked_cout(line0);
            std::this_thread::sleep_for(std::chrono::seconds(2));
            times = printCurrentTime();
            line1 = times +  "Process : " + std::to_string(P->processName) + " releases buffer : " + std::to_string(bufferToAcquire->serialNum);
            locked_cout(line1);
            bufferToAcquire->isBusy = false;
            freeList->attachToTailInFreeList(bufferToAcquire);
            print();
          }
          bufferToAcquire->marked = 1;
          return false;
        }
      }
      else {
          times = printCurrentTime();                                                                                                                                  // busy
          line0 = times +  "Process : " + std::to_string(P->processName) + " found buffer : " + std::to_string(bufferToAcquire->serialNum) + " but busy. Waiting...";
          locked_cout(line0);
          bufferToAcquire->bufferCondVar.wait(lock, [&]{return !bufferToAcquire->isBusy;});
          times  = printCurrentTime();
          line1 = times +  " Buffer : " + std::to_string(bufferToAcquire->serialNum) + " is free now. Process : " + std::to_string(P->processName) + " starts search again";
          locked_cout(line1);
          lock.unlock();
          return true;
      }
    }
    else {
      std::unique_lock<std::mutex> lock(bufferToAcquire->bufferMutex);
      bufferToAcquire->isBusy = true;
      times = printCurrentTime();                                                                                                            // marked busy
      line0 = times +  "Process : " + std::to_string(P->processName) + " did not find in hashList . Took one from freeList";
      locked_cout(line0);
      if(bufferToAcquire->marked == 1) {
        times = printCurrentTime();
        line1 = times + "Process : " + std::to_string(P->processName) + " found buffer : " + std::to_string(bufferToAcquire->serialNum) + " from free list but marked delayed-write. Starting asynchronous write from buffer : " + std::to_string(bufferToAcquire->serialNum);
        locked_cout(line1);
        bufferToAcquire->marked = 0;
        std::thread t(&BufferCache::writeToDisk,this,bufferToAcquire);
        t.detach();
        times = printCurrentTime();
        std::string line2 = times + "Process : " +  std::to_string(P->processName) + " takes next buffer from freeList";
        locked_cout(line2);
        acquire(bufferToAcquire->freeListNext,P);
      }
      else {
        int hashedVal = hash(P->processBlockNum,P->processDevNum);
        int currentList = hash(bufferToAcquire->blockNum,bufferToAcquire->deviceNum);
        times = printCurrentTime();
        line0 = times + "Process : " + std::to_string(P->processName) + " reassigns buffer : " + std::to_string(bufferToAcquire->serialNum) + "\'s block number and process number" ;
        locked_cout(line0);
        bufferToAcquire->deviceNum = P->processDevNum;
        bufferToAcquire->blockNum = P->processBlockNum;
        if(freeList->isEmpty) {
          std::unique_lock<std::mutex> lock2(freeListMutex);
          hashQueuesArray[currentList]->detachFromHashList(bufferToAcquire);
          hashQueuesArray[hashedVal]->attachToTailInHashList(bufferToAcquire);
          times = printCurrentTime();
          line1 = times +  "Process : " + std::to_string(P->processName) + " working on  buffer : " + std::to_string(bufferToAcquire->serialNum);
          locked_cout(line1);
          std::this_thread::sleep_for(std::chrono::seconds(2));
          times  = printCurrentTime();
          std::string line2 = times + "Process : " + std::to_string(P->processName) + " releases buffer : " + std::to_string(bufferToAcquire->serialNum);
          locked_cout(line2);
          bufferToAcquire->isBusy = false;
          freeList->attachToTailInFreeList(bufferToAcquire);
          print();
          freeListCondVar.notify_all();
          lock2.unlock();
          bufferToAcquire->bufferCondVar.notify_all();
          lock.unlock();
        }
        else {
          bufferToAcquire->isBusy = true;
          int hashedVal = hash(P->processBlockNum,P->processDevNum);
          int currentList = hash(bufferToAcquire->blockNum,bufferToAcquire->deviceNum);
          hashQueuesArray[currentList]->detachFromHashList(bufferToAcquire);
          hashQueuesArray[hashedVal]->attachToTailInHashList(bufferToAcquire);
          times = printCurrentTime();
          line0 = times +  "Process : " + std::to_string(P->processName) + " working on  buffer : " + std::to_string(bufferToAcquire->serialNum);
          locked_cout(line0);
          std::this_thread::sleep_for(std::chrono::seconds(2));
          times = printCurrentTime();
          line0 = times + "Process : " + std::to_string(P->processName) + " releases buffer : " + std::to_string(bufferToAcquire->serialNum);
          locked_cout(line0);
          bufferToAcquire->isBusy = false;
          freeList->attachToTailInFreeList(bufferToAcquire);
          print();
          bufferToAcquire->bufferCondVar.notify_all();
          lock.unlock();
        }
        bufferToAcquire->marked = 1;
        return false;
      }
    }
  }
  else {
    times = printCurrentTime();
    line0 = times +  "Process : " + std::to_string(P->processName) + " requires buffer that is not on its hashList and the free List is empty. Waiting for any buffer to become free";
    locked_cout(line0);
    std::unique_lock<std::mutex> lock2(freeListMutex);
    freeListCondVar.wait(lock2);
    // some buffer was added to freeList. Starts search again
    times = printCurrentTime();
    line1 = times +  " Process : " + std::to_string(P->processName) + " starts search again as some buffer has become free";
    locked_cout(line1);
    return true;
  }
}

//+++++++++++++class : Kernel ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

class Kernel {
  private:
    BufferCache *bCache;

  public:
    Kernel(int numBuffers,int numHashQueues) {
      bCache = new BufferCache(numBuffers,numHashQueues);
    }
    void assign(Process*);
};

//==================Kernel function definitions=======================================================================================================

void Kernel::assign(Process *p) {
  std::string times = printCurrentTime();
  std::string line0 = times + "Process : " + std::to_string(p->processName) + " begins search";
  locked_cout(line0);
  bool searching = true;
  while(searching) {
    Buffer* bufferToAcquire = bCache->search(p->processBlockNum,p->processDevNum);
    searching = bCache->acquire(bufferToAcquire,p);
  }
  times = printCurrentTime();
  std::string line1 = times + "Process : " + std::to_string(p->processName) + " exits.";
  locked_cout(line1);
}



//+++++++++++++++++++++++++++++++++++Driver Function+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


int main() {
  std::time_t currentTime;
  int numThreads = 7;
  std::thread t[numThreads];
  Process *P[numThreads];
  std::srand(std::time(nullptr));
  Kernel K(4,2);
  currentTime = std::time(nullptr);
  std::string times; 
  times = printCurrentTime();
  std::string line0 = times + "Program begins.";
  locked_cout(line0);
  for (int i = 0; i < numThreads; i++) {
    P[i] = new Process(2,1,char(i+65));
    t[i] = std::thread(&Kernel::assign,&K,P[i]);
  }
  for (int i = 0; i < numThreads; ++i) {
    t[i].join();
  }
  times = printCurrentTime();
 line0 = times + "Program stops.";
  locked_cout(line0);
  return 0;
}
