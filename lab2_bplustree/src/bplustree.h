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
    void InsertInternal(Node* current, const Key& key, Node*& new_child, Key& new_key);

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
        LeafNode<Key>* leaf = FindLeaf(key);

        // 리프 노드에 키를 정렬된 순서로 삽입
        auto itr = std::lower_bound(leaf->keys.begin(), leaf->keys.end(), key);
        leaf->keys.insert(itr, key);

        // 리프 노드의 키 개수가 차수(degree) 이상인지 확인 (오버플로우)
        if (leaf->keys.size() >= degree) {
            // 리프 노드를 분할
            LeafNode<Key>* new_leaf = new LeafNode<Key>();
            int mid = (leaf->keys.size() + 1) / 2;

            // 기존 리프 노드의 키들 중 후반부를 새 리프 노드로 이동
            new_leaf->keys.assign(leaf->keys.begin() + mid, leaf->keys.end());
            leaf->keys.erase(leaf->keys.begin() + mid, leaf->keys.end());

            // 리프 노드의 연결 리스트 포인터 업데이트
            new_leaf->next = leaf->next;
            leaf->next = new_leaf;

            // 부모 노드로 푸시할 키 준비 (새 리프 노드의 첫 번째 키)
            Key new_key = new_leaf->keys.front();

            // 현재 리프 노드가 루트 노드인 경우, 새로운 루트를 생성
            if (leaf == root) {
                InternalNode<Key>* new_root = new InternalNode<Key>();
                new_root->keys.push_back(new_key);
                new_root->children.push_back(leaf);
                new_root->children.push_back(new_leaf);
                root = new_root;
            } else {
                // 부모 노드를 찾는 로직
                InternalNode<Key>* parent = nullptr;
                void* current = root;
                // 루트부터 시작하여 리프 노드의 부모를 찾음
                while (!isLeaf(current)) {
                    InternalNode<Key>* internal = static_cast<InternalNode<Key>*>(current);
                    for (size_t i = 0; i < internal->children.size(); ++i) {
                        if (internal->children[i] == leaf) {
                            parent = internal; // 부모 노드 발견
                            break;
                        }
                    }
                    if (parent) break; // 부모를 찾았으면 루프 종료
                    // 다음 레벨로 이동: 키를 기준으로 적절한 자식 노드로 이동
                    auto itr = std::lower_bound(internal->keys.begin(), internal->keys.end(), key);
                    int idx = itr - internal->keys.begin();
                    current = internal->children[idx];
                }

                // 부모 노드에 새 키와 새 리프 노드를 삽입
                if (parent) {
                    InsertInternal(parent, new_key, new_leaf);
                }
            }
        }
    }
    // To be implemented by students
}


// Contains function: Checks if a key exists in the B+ Tree.
template<typename Key>
bool Bplustree<Key>::Contains(const Key& key) const {
    // TODO: Implement lookup logic here.
     // 키가 존재해야 할 리프 노드를 탐색
    LeafNode<Key>* leaf = FindLeaf(key);

    // 리프 노드가 없으면 트리가 비어 있거나 키가 존재하지 않음
    if (!leaf) {
        return false;
    }

    // 리프 노드의 키들에서 이진 탐색으로 키를 찾음
    auto itr = std::lower_bound(leaf->keys.begin(), leaf->keys.end(), key);

    // 찾은 위치가 유효하고, 해당 위치의 키가 주어진 키와 같은지 확인
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
    // To be implemented by students
    return false;
}


// InsertInternal function: Helper function to insert a key into an internal node.
template<typename Key>
void Bplustree<Key>::InsertInternal(Node* current, const Key& key, Node*& new_child, Key& new_key) {
    // TODO: Implement internal node insertion logic here.
    // 키를 정렬된 위치에 삽입
    auto itr = std::lower_bound(node->keys.begin(), node->keys.end(), key);
    int idx = itr - node->keys.begin();
    node->keys.insert(itr, key);
    // 새 자식 노드를 키의 오른쪽에 삽입 (B+ 트리에서 자식은 키보다 하나 더 많음)
    node->children.insert(node->children.begin() + idx + 1, child);

    // 오버플로우 확인: 노드의 키 개수가 차수(degree)를 초과하는 경우
    if (node->keys.size() > degree) {
        // 내부 노드를 분할
        InternalNode<Key>* new_internal = new InternalNode<Key>();
        int mid = (node->keys.size() + 1) / 2;

        // 키와 자식의 후반부를 새 내부 노드로 이동
        new_internal->keys.assign(node->keys.begin() + mid, node->keys.end());
        new_internal->children.assign(node->children.begin() + mid, node->children.end());
        // 부모로 올라갈 키는 기존 노드에서 중간 키를 선택
        Key new_key = *(node->keys.begin() + mid - 1);
        // 기존 노드에서 이동된 키와 자식 제거
        node->keys.erase(node->keys.begin() + mid - 1, node->keys.end());
        node->children.erase(node->children.begin() + mid, node->children.end());

        // 현재 노드가 루트 노드인 경우, 새로운 루트를 생성
        if (node == root) {
            InternalNode<Key>* new_root = new InternalNode<Key>();
            new_root->keys.push_back(new_key);
            new_root->children.push_back(node);
            new_root->children.push_back(new_internal);
            root = new_root;
        } else {
            // 부모 노드를 찾는 로직
            InternalNode<Key>* parent = nullptr;
            void* current = root;
            // 루트부터 시작하여 현재 노드(node)가 자식으로 포함된 부모 노드를 찾음
            while (current && !isLeaf(current)) {
                InternalNode<Key>* internal = static_cast<InternalNode<Key>*>(current);
                // 현재 내부 노드의 자식들 중 node가 있는지 확인
                for (size_t i = 0; i < internal->children.size(); ++i) {
                    if (internal->children[i] == node) {
                        parent = internal; // 부모 노드 발견
                        break;
                    }
                }
                if (parent) break; // 부모를 찾았으면 루프 종료
                // 다음 레벨로 이동: 키를 기준으로 적절한 자식 노드로 이동
                auto itr = std::lower_bound(internal->keys.begin(), internal->keys.end(), new_key);
                int idx = itr - internal->keys.begin();
                // 자식 노드가 존재하고 내부 노드인 경우에만 이동
                if (idx < internal->children.size() && !isLeaf(internal->children[idx])) {
                    current = internal->children[idx];
                } else {
                    break; // 더 이상 탐색할 수 없음
                }
            }

            // 부모 노드가 존재하면 부모 노드에 새 키와 새 내부 노드를 삽입
            if (parent) {
                // 부모 노드에 키와 자식을 삽입
                auto parent_itr = std::lower_bound(parent->keys.begin(), parent->keys.end(), new_key);
                int parent_idx = parent_itr - parent->keys.begin();
                parent->keys.insert(parent_itr, new_key);
                parent->children.insert(parent->children.begin() + parent_idx + 1, new_internal);

                // 부모 노드의 오버플로우 확인
                if (parent->keys.size() > degree) {
                    // 부모 노드를 분할
                    InternalNode<Key>* new_parent_internal = new InternalNode<Key>();
                    int parent_mid = (parent->keys.size() + 1) / 2;

                    // 부모 노드의 키와 자식의 후반부를 새 내부 노드로 이동
                    new_parent_internal->keys.assign(parent->keys.begin() + parent_mid, parent->keys.end());
                    new_parent_internal->children.assign(parent->children.begin() + parent_mid, parent->children.end());
                    // 상위 부모로 올라갈 키 선택
                    Key parent_new_key = *(parent->keys.begin() + parent_mid - 1);
                    // 기존 부모 노드에서 이동된 키와 자식 제거
                    parent->keys.erase(parent->keys.begin() + parent_mid - 1, parent->keys.end());
                    parent->children.erase(parent->children.begin() + parent_mid, parent->children.end());

                    // 부모 노드가 루트 노드인 경우, 새로운 루트를 생성
                    if (parent == root) {
                        InternalNode<Key>* new_root = new InternalNode<Key>();
                        new_root->keys.push_back(parent_new_key);
                        new_root->children.push_back(parent);
                        new_root->children.push_back(new_parent_internal);
                        root = new_root;
                    } else {
                        // 상위 부모 노드를 찾아 재귀적으로 InsertInternal 호출
                        InsertInternal(parent, parent_new_key, new_parent_internal);
                    }
                }
            } else {
                // 부모 노드를 찾지 못한 경우 (트리 구조상 발생하지 않아야 함)
                throw std::runtime_error("부모 노드를 찾을 수 없습니다.");
            }
        }
    }
    // To be implemented by students
}


// DeleteInternal function: Helper function to delete a key from an internal node.
template<typename Key>
bool Bplustree<Key>::DeleteInternal(Node* current, const Key& key) {
    // TODO: Implement internal node deletion logic here.
    // To be implemented by students
    return false;
}


// FindLeaf function: Traverses the B+ Tree from the root to find the leaf node that should contain the given key.
template<typename Key>
typename Bplustree<Key>::LeafNode* Bplustree<Key>::FindLeaf(const Key& key) const {
    // TODO: Implement the traversal logic to locate the correct leaf node.
    Node* current = root;
    while (!current->is_leaf) {
        InternalNode* internal = current->as_internal();
        auto itr = std::upper_bound(internal->keys.begin(), internal->keys.end(), key);
        int pos = std::distance(internal->keys.begin(), itr);
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