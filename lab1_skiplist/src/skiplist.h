#include <stdlib.h>
#include <cstdint>
#include <algorithm>
#include <cmath>
#include <cstring>
#include <xmmintrin.h>
#include <immintrin.h>
#include <emmintrin.h>
#include <smmintrin.h>
#include <bit>
#include <functional>
#include <mutex>
#include <vector>
#include <atomic>

typedef std::chrono::high_resolution_clock Clock;

// Key is an 8-byte integer
typedef uint64_t Key;

int compare_(const Key& a, const Key& b) {
    if (a < b) {
        return -1;
    } else if (a > b) {
        return +1;
    } else {
        return 0;
    }
}

template<typename Key>
class SkipList {
   private:
    struct Node;

   public:
    SkipList(int max_level = 16, float probability = 0.5);

    void Insert(const Key& key); // Insertion function (to be implemented by students)
    bool Contains(const Key& key) const; // Lookup function (to be implemented by students)
    std::vector<Key> Scan(const Key& key, const int scan_num); // Range query function (to be implemented by students)
    bool Delete(const Key& key) const; // Delete function (to be implemented by students)
    void Print() const;

   private:
    int RandomLevel(); // Generates a random level for new nodes (to be implemented by students)

    Node* head; // Head node (starting point of the SkipList)
    int max_level; // Maximum level in the SkipList
    float probability; // Probability factor for level increase
};

// SkipList Node structure
template<typename Key>
struct SkipList<Key>::Node {
    Key key;
    std::vector<Node*> next; // Pointer array for multiple levels
    // Constructor for Node
    Node(Key key, int level);
};

template<typename Key>
SkipList<Key>::Node::Node(Key key, int level): key(key), next(level) {
}; // node 생성자 추가

// Generate a random level for new nodes
template<typename Key>
int SkipList<Key>::RandomLevel() {
    int level = 1;
    while ((float)rand() / RAND_MAX < probability && level < max_level) {
        level++; 
        //rand 함수를 이용해 0~1의 난수를 생성하고 이 수가 probability보다 작고 level이 max_level보다 작을 때 level을 1 증가시킨다.
    }
    return level;
}

// Constructor for SkipList
template<typename Key>
SkipList<Key>::SkipList(int max_level, float probability)
    : max_level(max_level), probability(probability) {
        head = new Node(0, max_level);
        //head에 key value가 0이고 max_level이 max_level인 노드 생성
        head->next = std::vector<Node*>(max_level, nullptr);
        //head의 next에 max_level만큼 노드 배열을 nullptr로 초기화
    // To be implemented by students
}

// Insert function (inserts a key into SkipList)
template<typename Key>
void SkipList<Key>::Insert(const Key& key) {
    
    // To be implemented by students
    // 각 레벨에서 업데이트해야 하는 노드를 저장하는 벡터
    std::vector<Node*> update(max_level, nullptr);
    Node* current = head;

    //레벨을 따라 적절한 삽입 위치 탐색
    for (int i = max_level - 1; i >= 0; i--) {
        while (current->next[i] != nullptr && current->next[i]->key < key) {
            current = current->next[i];
        }
        update[i] = current; // 레벨에 적절한 삽입 위치 저장
    }

    current = current->next[0];

    // 키가 존재하지 않을 시 삽입
    if (current == nullptr || current->key != key) {
        int new_level = RandomLevel();
        Node* new_node = new Node(key, new_level);

        // 레벨마다 포인터 변경
        for (int i = 0; i < new_level; i++) {
            new_node->next[i] = update[i]->next[i];
            update[i]->next[i] = new_node;
        }
    }
}

// Delete function (removes a key from SkipList)
template<typename Key>
bool SkipList<Key>::Delete(const Key& key) const {
    Node* current = head;
    std::vector<Node*> updates(max_level);

    //레벨을 따라 진행하며 찾아가는 과정
    for (int i = max_level - 1; i >= 0; i--) {
        while (current->next[i] != nullptr && current->next[i]->key < key) {
            current = current->next[i];
        }
        updates[i] = current;
    }

    current = current->next[0];
    if (current == nullptr || current->key != key) {
        return false;  // 키가 존재하지 않을 시 false 리턴
    }

    //전체 레벨에 있는 노드들을 삭제
    for (int i = 0; i < max_level; i++) {
        if (updates[i]->next[i] == current) {
            updates[i]->next[i] = current->next[i];
        } //update에 저장된 노드들의 다음 노드를 current의 다음 노드로 대체
        //상위 레벨이 없을 시 올라가지 않는다.
        if (updates[i]->next[i] == nullptr) break;
    }
    delete current;
    return true;
}

// Lookup function (checks if a key exists in SkipList)
template<typename Key>
bool SkipList<Key>::Contains(const Key& key) const {
    // To be implemented by students
        Node* current = head;

    //레벨을 따라 진행하며 찾아가는 과정
    for (int i = max_level - 1; i >= 0; i--) {
        while (current->next[i] != nullptr && current->next[i]->key < key) {
            current = current->next[i];
        }
    }
    current = current->next[0];
    if(current != nullptr && current->key == key){
        return true;
    } //리스트에 요소가 존재할 시 true 리턴
    return false; //리스트에 요소가 존재하지 않을 시 false 리턴
}

// Range query function (retrieves scan_num keys starting from key)
template<typename Key>
std::vector<Key> SkipList<Key>::Scan(const Key& key, const int scan_num) {
    // To be implemented by students
    std::vector<Key> result;
    Node* current = head;

    // 키와 동일하거나 키보다 큰 노드를 찾아 순회
    for (int i = max_level - 1; i >= 0; i--) {
        while (current->next[i] != nullptr && current->next[i]->key < key) {
            current = current->next[i];
        }
    }
    current = current->next[0];
    //키 값과 가장 근접한 노드의 다음 노드의 값 current 설정
    while (current != nullptr && result.size() < scan_num) {
        result.push_back(current->key);
        current = current->next[0];
    }
    //범위 스캔값 result에 push

    return result;
}

template<typename Key>
void SkipList<Key>::Print() const {

  std::cout << "SkipList Structure:\n";

  for (int level = max_level - 1; level >= 0; --level) {

    Node* node = head->next[level];

    std::cout << "Level " << level << ": ";

    while (node != nullptr) {

      std::cout << node->key << " ";

      node = node->next[level];

    }

    std::cout << "\n";

  }

}