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

// Define Clock and Key types
typedef std::chrono::high_resolution_clock Clock;
typedef uint64_t Key;

// Compare function for keys
int compare_(const Key& a, const Key& b) {
    if (a < b) {
        return -1;
    } else if (a > b) {
        return +1;
    } else {
        return 0;
    }
}

// B+ Tree class template definition
template<typename Key>
class Bplustree {
   private:
    // Forward declaration of node structures
    struct Node;
    struct InternalNode;
    struct LeafNode;

   public:
    // Constructor: Initializes a B+ Tree with the specified degree (maximum number of children per internal node)
    Bplustree(int degree = 4);

    // Insert function:
    // Inserts a key into the B+ Tree.
    // TODO: Implement insertion, handling leaf node insertion and splitting if necessary.
    void Insert(const Key& key);

    // Contains function:
    // Returns true if the key exists in the tree; otherwise, returns false.
    // TODO: Implement key lookup starting from the root and traversing to the appropriate leaf.
    bool Contains(const Key& key) const;

    // Scan function:
    // Performs a range query starting from the specified key and returns up to 'scan_num' keys.
    // TODO: Traverse leaf nodes using the next pointer and collect keys.
    std::vector<Key> Scan(const Key& key, const int scan_num);

    // Delete function:
    // Removes the specified key from the tree.
    // TODO: Implement deletion, handling key removal, merging, or rebalancing nodes if required.
    bool Delete(const Key& key);

    // Print function:
    // Traverses and prints the internal structure of the B+ Tree.
    // This function is helpful for debugging and verifying that the tree is constructed correctly.
    void Print() const;

   private:
    // Base Node structure. All nodes (internal and leaf) derive from this.
    struct Node {
        bool is_leaf; // Indicates whether the node is a leaf
        // Helper functions to cast a Node pointer to InternalNode or LeafNode pointers.
        InternalNode* as_internal() { return static_cast<InternalNode*>(this); }
        LeafNode* as_leaf() { return static_cast<LeafNode*>(this); }
        virtual ~Node() = default;
    };

    // Internal node structure for the B+ Tree.
    // Stores keys and child pointers.
    struct InternalNode : public Node {
        std::vector<Key> keys;         // Keys used to direct search to the correct child
        std::vector<Node*> children;   // Pointers to child nodes
        InternalNode() { this->is_leaf = false; }
    };

    // Leaf node structure for the B+ Tree.
    // Stores actual keys and a pointer to the next leaf for efficient range queries.
    struct LeafNode : public Node {
        std::vector<Key> keys; // Keys stored in the leaf node
        LeafNode* next;        // Pointer to the next leaf node for range scanning
        LeafNode() : next(nullptr) { this->is_leaf = true; }
    };

    // Helper function to insert a key into an internal node.
    // 'new_child' and 'new_key' are output parameters if the node splits.
    // TODO: Implement insertion into an internal node and handle splitting of nodes.
    void Bplustree<Key>::InsertInternal(InternalNode* node, const Key& key, Node* child);

    // Helper function to delete a key from the tree recursively.
    // TODO: Implement deletion from internal nodes with proper merging or rebalancing.
    bool DeleteInternal(Node* current, const Key& key);

    // Helper function to find the leaf node where the key should reside.
    // TODO: Implement traversal from the root to the appropriate leaf node.
    LeafNode* FindLeaf(const Key& key) const;

    // Helper function to recursively print the tree structure.
    void PrintRecursive(const Node* node, int level) const;

    Node* root;   // Root node of the B+ Tree
    int degree;   // Maximum number of children per internal node
};

// Constructor implementation
// Initializes the tree by creating an empty leaf node as the root.
template<typename Key>
Bplustree<Key>::Bplustree(int degree) : degree(degree) {
    root = new LeafNode();
    // To be implemented by students
}

// Insert function: Inserts a key into the B+ Tree.
template<typename Key>
void Bplustree<Key>::Insert(const Key& key) {
    // TODO: Implement insertion logic here.
    // 키를 삽입할 리프 노드를 찾음
    LeafNode* leaf = FindLeaf(key);

    // 리프 노드에 키를 정렬된 순서로 삽입
    auto itr = std::lower_bound(leaf->keys.begin(), leaf->keys.end(), key);
    leaf->keys.insert(itr, key);

    // 오버플로우 확인
    if (leaf->keys.size() > degree) {
        // 리프 노드를 분할
        LeafNode* new_leaf = new LeafNode();
        int mid = (leaf->keys.size() + 1) / 2;

        // 키의 후반부를 새 리프 노드로 이동
        new_leaf->keys.assign(leaf->keys.begin() + mid, leaf->keys.end());
        leaf->keys.erase(leaf->keys.begin() + mid, leaf->keys.end());

        // 연결 리스트 포인터 업데이트
        new_leaf->next = leaf->next;
        leaf->next = new_leaf;

        // 부모 노드로 푸시할 키 준비
        Key new_key = new_leaf->keys.front();

        // 리프 노드가 루트인 경우, 새로운 루트를 생성
        if (leaf == root) {
            InternalNode* new_root = new InternalNode();
            new_root->keys.push_back(new_key);
            new_root->children.push_back(leaf);
            new_root->children.push_back(new_leaf);
            root = new_root;
        } else {
            // 부모 노드에 새 키와 자식을 삽입
            InternalNode* parent = FindParent(root, leaf);
            InsertInternal(parent, new_key, new_leaf);
        }
    }
    // To be implemented by students
}


// Contains function: Checks if a key exists in the B+ Tree.
template<typename Key>
bool Bplustree<Key>::Contains(const Key& key) const {
    // TODO: Implement lookup logic here.
    // 키가 존재해야 할 리프 노드를 탐색
    LeafNode* leaf = FindLeaf(key);

    // 리프 노드가 없으면 키가 존재하지 않음
    if (!leaf) {
        return false;
    }

    // 리프 노드의 키들에서 이진 탐색으로 키를 찾음
    auto itr = std::lower_bound(leaf->keys.begin(), leaf->keys.end(), key);

    //찾은 키가 주어진 키와 같은지 확인
    return itr != leaf->keys.end() && *itr == key;
    // To be implemented by students
}
   


// Scan function: Performs a range query starting from a given key.
template<typename Key>
std::vector<Key> Bplustree<Key>::Scan(const Key& key, const int scan_num) {
    // TODO: Implement range query logic here.
    //결과 벡터 선언
    std::vector<Key> result;
    result.reserve(scan_num);

    // 주어진 키 이상의 키가 포함된 리프 노드를 찾음
    LeafNode* leaf = FindLeaf(key);

    // 시작 키의 위치를 찾음
    auto it = std::lower_bound(leaf->keys.begin(), leaf->keys.end(), key);
    int count = 0;

    // 찾은 위치부터 최대 scan_num 개의 키를 수집
    while (it != leaf->keys.end() && count < scan_num) {
        result.push_back(*it);
        ++it;
        ++count;

        // 현재 리프 노드의 끝에 도달하고 다음 리프 노드가 존재하면 이동
        if (it == leaf->keys.end() && leaf->next != nullptr) {
            leaf = leaf->next;
            it = leaf->keys.begin();
        }
    }

    return result;
    // To be implemented by students
}


// Delete function: Removes a key from the B+ Tree.
template<typename Key>
bool Bplustree<Key>::Delete(const Key& key) {
    // TODO: Implement deletion logic here.
    // 키가 있어야 할 리프 노드를 찾음
    LeafNode* leaf = FindLeaf(key);
    if (!leaf) {
        return false; // 리프 노드가 없으면 삭제 실패
    }

    // 리프 노드에서 키를 찾음
    auto it = std::lower_bound(leaf->keys.begin(), leaf->keys.end(), key);
    if (it == leaf->keys.end() || *it != key) {
        return false; // 키가 없으면 삭제 실패
    }

    // 키를 리프 노드에서 제거
    leaf->keys.erase(it);

    // 언더플로우 확인: 최소 키 수는 (degree + 1) / 2
    if (leaf->keys.size() < (degree + 1) / 2) {
        if (leaf == root) {
            // 루트가 리프 노드이고 키가 모두 제거된 경우, 빈 트리로 설정
            if (leaf->keys.empty()) {
                delete root;
                root = nullptr;
            }
        } else {
            // 부모 노드를 찾아 언더플로우 처리
            InternalNode* parent = FindParent(root, leaf);
            if (!parent) {
                return false; // 부모 노드가 없으면 삭제 실패
            }
            DeleteInternal(parent, leaf, key);
        }
    }

    return true; // 삭제 성공
    // To be implemented by students
}


// InsertInternal function: Helper function to insert a key into an internal node.
template<typename Key>
void Bplustree<Key>::InsertInternal(InternalNode* node, const Key& key, Node* child) {
    // TODO: Implement internal node insertion logic here.
    // 새 키를 삽입할 위치를 찾음
    auto itr = std::lower_bound(node->keys.begin(), node->keys.end(), key);
    int pos = std::distance(node->keys.begin(), itr);

    // 키와 자식을 삽입
    node->keys.insert(node->keys.begin() + pos, key);
    node->children.insert(node->children.begin() + pos + 1, child);

    // 오버플로우 확인
    if (node->keys.size() > degree) {
        // 내부 노드를 분할
        InternalNode* new_node = new InternalNode();
        int mid = (node->keys.size() + 1) / 2;

        // 키와 자식의 후반부를 새 노드로 이동
        new_node->keys.assign(node->keys.begin() + mid, node->keys.end());
        new_node->children.assign(node->children.begin() + mid, node->children.end());
        node->keys.erase(node->keys.begin() + mid, node->keys.end());
        node->children.erase(node->children.begin() + mid, node->children.end());

        // 중간 키를 부모로 푸시
        Key new_key = node->keys.back();
        node->keys.pop_back();

        // 노드가 루트인 경우, 새로운 루트를 생성
        if (node == root) {
            InternalNode* new_root = new InternalNode();
            new_root->keys.push_back(new_key);
            new_root->children.push_back(node);
            new_root->children.push_back(new_node);
            root = new_root;
        } else {
            // 부모 노드에 새 키와 자식을 삽입
            InternalNode* parent = FindParent(root, node);
            InsertInternal(parent, new_key, new_node);
        }
    }
    // To be implemented by students
}


// DeleteInternal function: Helper function to delete a key from an internal node.
template<typename Key>
bool Bplustree<Key>::DeleteInternal(Node* current, const Key& key) {
    // TODO: Implement internal node deletion logic here.
    // 노드의 인덱스를 찾음
    int index = 0;
    for (; index < parent->children.size(); ++index) {
        if (parent->children[index] == node) {
            break;
        }
    }

    int min_keys = (degree + 1) / 2;
    bool is_leaf = node->is_leaf;

    // 형제 노드 선택: 좌측 또는 우측 형제
    InternalNode* left_sibling = (index > 0) ? parent->children[index - 1]->as_internal() : nullptr;
    InternalNode* right_sibling = (index < parent->children.size() - 1) ? parent->children[index + 1]->as_internal() : nullptr;

    // 1. 좌측 형제로부터 키 빌리기
    if (left_sibling && left_sibling->keys.size() > min_keys) {
        if (is_leaf) {
            LeafNode* leaf = static_cast<LeafNode*>(node);
            LeafNode* left_leaf = static_cast<LeafNode*>(left_sibling);
            leaf->keys.insert(leaf->keys.begin(), left_leaf->keys.back());
            left_leaf->keys.pop_back();
            parent->keys[index - 1] = leaf->keys.front();
        } else {
            InternalNode* internal = static_cast<InternalNode*>(node);
            internal->keys.insert(internal->keys.begin(), parent->keys[index - 1]);
            parent->keys[index - 1] = left_sibling->keys.back();
            internal->children.insert(internal->children.begin(), left_sibling->children.back());
            left_sibling->children.pop_back();
            left_sibling->keys.pop_back();
        }
        return true;
    }

    // 2. 우측 형제로부터 키 빌리기
    if (right_sibling && right_sibling->keys.size() > min_keys) {
        if (is_leaf) {
            LeafNode* leaf = static_cast<LeafNode*>(node);
            LeafNode* right_leaf = static_cast<LeafNode*>(right_sibling);
            leaf->keys.push_back(right_leaf->keys.front());
            right_leaf->keys.erase(right_leaf->keys.begin());
            parent->keys[index] = right_leaf->keys.front();
        } else {
            InternalNode* internal = static_cast<InternalNode*>(node);
            internal->keys.push_back(parent->keys[index]);
            parent->keys[index] = right_sibling->keys.front();
            internal->children.push_back(right_sibling->children.front());
            right_sibling->children.erase(right_sibling->children.begin());
            right_sibling->keys.erase(right_sibling->keys.begin());
        }
        return true;
    }

    // 3. 병합: 좌측 또는 우측 형제와 병합
    if (left_sibling) {
        if (is_leaf) {
            LeafNode* leaf = static_cast<LeafNode*>(node);
            LeafNode* left_leaf = static_cast<LeafNode*>(left_sibling);
            left_leaf->keys.insert(left_leaf->keys.end(), leaf->keys.begin(), leaf->keys.end());
            left_leaf->next = leaf->next;
            delete leaf;
        } else {
            InternalNode* internal = static_cast<InternalNode*>(node);
            left_sibling->keys.push_back(parent->keys[index - 1]);
            left_sibling->keys.insert(left_sibling->keys.end(), internal->keys.begin(), internal->keys.end());
            left_sibling->children.insert(left_sibling->children.end(), internal->children.begin(), internal->children.end());
            delete internal;
        }
        parent->keys.erase(parent->keys.begin() + (index - 1));
        parent->children.erase(parent->children.begin() + index);
    } else if (right_sibling) {
        if (is_leaf) {
            LeafNode* leaf = static_cast<LeafNode*>(node);
            LeafNode* right_leaf = static_cast<LeafNode*>(right_sibling);
            leaf->keys.insert(leaf->keys.end(), right_leaf->keys.begin(), right_leaf->keys.end());
            leaf->next = right_leaf->next;
            delete right_leaf;
        } else {
            InternalNode* internal = static_cast<InternalNode*>(node);
            internal->keys.push_back(parent->keys[index]);
            internal->keys.insert(internal->keys.end(), right_sibling->keys.begin(), right_sibling->keys.end());
            internal->children.insert(internal->children.end(), right_sibling->children.begin(), right_sibling->children.end());
            delete right_sibling;
        }
        parent->keys.erase(parent->keys.begin() + index);
        parent->children.erase(parent->children.begin() + (index + 1));
    }

    // 부모 노드의 언더플로우 확인
    if (parent != root && parent->keys.size() < min_keys) {
        InternalNode* grandparent = FindParent(root, parent);
        if (grandparent) {
            DeleteInternal(grandparent, parent, parent->keys.empty() ? key : parent->keys.front());
        }
    } else if (parent == root && parent->keys.empty() && !parent->children.empty()) {
        // 루트가 단일 자식만 가지면 트리 높이를 줄임
        Node* temp = root;
        root = parent->children.front();
        delete temp;
    }

    return true;
    // To be implemented by students
}


// FindLeaf function: Traverses the B+ Tree from the root to find the leaf node that should contain the given key.
template<typename Key>
typename Bplustree<Key>::LeafNode* Bplustree<Key>::FindLeaf(const Key& key) const {
    // TODO: Implement the traversal logic to locate the correct leaf node.
    Node* current = root;
    while (!current->is_leaf) {
        InternalNode* internal = current->as_internal();
        auto it = std::upper_bound(internal->keys.begin(), internal->keys.end(), key);
        int pos = std::distance(internal->keys.begin(), it);
        current = internal->children[pos];
    }
    return current->as_leaf();
    // To be implemented by students
}

// Print function: Public interface to print the B+ Tree structure.
template<typename Key>
void Bplustree<Key>::Print() const {
    PrintRecursive(root, 0);
}

// Helper function: Recursively prints the tree structure with indentation based on tree level.
template<typename Key>
void Bplustree<Key>::PrintRecursive(const Node* node, int level) const {
    if (node == nullptr) return;
    // Indent based on the level in the tree.
    for (int i = 0; i < level; ++i)
        std::cout << "  ";
    if (node->is_leaf) {
        // Print leaf node keys.
        const LeafNode* leaf = node->as_leaf();
        std::cout << "[Leaf] ";
        for (const Key& key : leaf->keys)
            std::cout << key << " ";
        std::cout << std::endl;
    } else {
        // Print internal node keys and recursively print children.
        const InternalNode* internal = node->as_internal();
        std::cout << "[Internal] ";
        for (const Key& key : internal->keys)
            std::cout << key << " ";
        std::cout << std::endl;
        for (const Node* child : internal->children)
            PrintRecursive(child, level + 1);
    }
}