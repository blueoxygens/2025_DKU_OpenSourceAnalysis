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
    LeafNode* leaf = FindLeaf(key);

    // 1. Leaf에 key 삽입 (정렬된 위치)
    auto it = std::lower_bound(leaf->keys.begin(), leaf->keys.end(), key);
    leaf->keys.insert(it, key);

    // 2. Overflow 체크
    if (leaf->keys.size() < degree) {
        // Overflow 안 났으면 끝
        return;
    }

    // 3. Overflow 났으면 Leaf Split
    LeafNode* new_leaf = new LeafNode();
    int mid = (degree + 1) / 2;

    // keys를 반으로 나눔
    new_leaf->keys.assign(leaf->keys.begin() + mid, leaf->keys.end());
    leaf->keys.resize(mid);

    // next 포인터 연결
    new_leaf->next = leaf->next;
    leaf->next = new_leaf;

    // 4. 부모에 새 key 등록
    Node* new_child = new_leaf;
    Key new_key = new_leaf->keys[0]; // 새 리프의 첫 번째 키를 부모로 올림

    // 만약 root였다면 새로운 root 생성
    if (leaf == root) {
        InternalNode* new_root = new InternalNode();
        new_root->keys.push_back(new_key);
        new_root->children.push_back(leaf);
        new_root->children.push_back(new_child);
        root = new_root;
        return;
    }

    // leaf의 부모 찾아서 InsertInternal 호출
    InsertInternal(root, new_key, new_child, new_key);
    // To be implemented by students
}


// Contains function: Checks if a key exists in the B+ Tree.
template<typename Key>
bool Bplustree<Key>::Contains(const Key& key) const {
    // TODO: Implement lookup logic here.
    LeafNode* leaf = FindLeaf(key);

    auto itr = std::lower_bound(leaf->keys.begin(), leaf->keys.end(), key);
    return (itr != leaf->keys.end() && *itr == key);
    // To be implemented by students
}


// Scan function: Performs a range query starting from a given key.
template<typename Key>
std::vector<Key> Bplustree<Key>::Scan(const Key& key, const int scan_num) {
    // TODO: Implement range query logic here.
    std::vector<Key> result;
    LeafNode* leaf = FindLeaf(key);

    // 1. 리프 안에서 key 이상인 곳부터 시작
    auto itr = std::lower_bound(leaf->keys.begin(), leaf->keys.end(), key);

    // 2. 리프들을 따라가면서 key들을 모은다
    while (leaf && result.size() < static_cast<size_t>(scan_num)) {
        for (; itr != leaf->keys.end() && result.size() < static_cast<size_t>(scan_num); ++itr) {
            result.push_back(*itr);
        }
        leaf = leaf->next;
        if (leaf) {
            itr = leaf->keys.begin();
        }
    }

    return result;
    // To be implemented by students
}


// Delete function: Removes a key from the B+ Tree.
template<typename Key>
bool Bplustree<Key>::Delete(const Key& key) {
    // TODO: Implement deletion logic here.
    // 1. 리프 노드를 찾아서 key 삭제
    LeafNode* leaf = FindLeaf(key);
    auto itr = std::lower_bound(leaf->keys.begin(), leaf->keys.end(), key);

    // 2. key가 없으면 삭제 실패
    if (itr == leaf->keys.end() || *itr != key) {
        return false;
    }
    leaf->keys.erase(itr);  // key 삭제

    // 3. 리프 노드 underflow 체크
    if (leaf->keys.size() < (degree - 1) / 2) {
        LeafNode* sibling = nullptr;
        bool is_left_sibling = false;

        InternalNode* parent = nullptr;
        Node* current = root;

        // 4. 부모 노드로부터 형제를 찾는다
        while (!current->is_leaf) {
            parent = current->as_internal();
            bool found = false;
            for (size_t i = 0; i < parent->keys.size(); ++i) {
                if (parent->keys[i] > key) {
                    current = parent->children[i];
                    found = true;
                    break;
                }
            }
            if (!found) {
                current = parent->children.back();
            }
        }

        // 부모에서 leaf 노드를 찾았다면, 형제 정보도 찾을 수 있다.
        for (size_t i = 0; i < parent->keys.size(); ++i) {
            if (parent->children[i]->as_leaf() == leaf) {
                if (i > 0) {
                    sibling = parent->children[i - 1]->as_leaf();
                    is_left_sibling = true;
                } else if (i < parent->keys.size()) {
                    sibling = parent->children[i + 1]->as_leaf();
                    is_left_sibling = false;
                }
                break;
            }
        }

        // 5. 형제에서 빌리기
        if (sibling) {
            if (sibling->keys.size() > (degree - 1) / 2) {
                if (is_left_sibling) {
                    leaf->keys.insert(leaf->keys.begin(), sibling->keys.back());
                    sibling->keys.pop_back();
                } else {
                    leaf->keys.push_back(sibling->keys.front());
                    sibling->keys.erase(sibling->keys.begin());
                }
                parent->keys[is_left_sibling ? parent->keys.size() - 1 : 0] = leaf->keys.front();
            } else {
                if (is_left_sibling) {
                    sibling->keys.insert(sibling->keys.end(), leaf->keys.begin(), leaf->keys.end());
                    parent->keys.pop_back();
                    parent->children.erase(parent->children.begin() + parent->keys.size());
                } else {
                    leaf->keys.insert(leaf->keys.end(), sibling->keys.begin(), sibling->keys.end());
                    parent->keys.pop_back();
                    parent->children.erase(parent->children.begin() + parent->keys.size());
                }
            }
        }
    }

    // 6. 부모 노드 갱신 및 루트 갱신
    if (root->as_internal()->children.size() == 1) {
        // 루트가 하나의 자식만 가질 경우 자식을 루트로 설정
        root = root->as_internal()->children[0];
    }

    return true;
    // To be implemented by students
}


// InsertInternal function: Helper function to insert a key into an internal node.
template<typename Key>
void Bplustree<Key>::InsertInternal(Node* current, const Key& key, Node*& new_child, Key& new_key) {
    // TODO: Implement internal node insertion logic here.
        InternalNode* internal = current->as_internal();

    // 1. key가 들어갈 위치 찾기 (lower_bound 이용)
    auto it = std::upper_bound(internal->keys.begin(), internal->keys.end(), key);
    int idx = it - internal->keys.begin();

    // 2. children에 new_child 삽입
    internal->keys.insert(internal->keys.begin() + idx, new_key);
    internal->children.insert(internal->children.begin() + idx + 1, new_child);

    // 3. Overflow 체크
    if (internal->keys.size() < degree) {
        return; // 아직 문제 없음
    }

    // 4. InternalNode Split
    InternalNode* new_internal = new InternalNode();
    int mid = (degree + 1) / 2;

    // keys와 children 나누기
    new_internal->keys.assign(internal->keys.begin() + mid + 1, internal->keys.end());
    new_internal->children.assign(internal->children.begin() + mid + 1, internal->children.end());

    new_key = internal->keys[mid]; // 중간 key를 부모로 올림

    // 기존 internal 정리
    internal->keys.resize(mid);
    internal->children.resize(mid + 1);

    new_child = new_internal;

    // 루트였으면 새로운 루트 생성
    if (current == root) {
        InternalNode* new_root = new InternalNode();
        new_root->keys.push_back(new_key);
        new_root->children.push_back(internal);
        new_root->children.push_back(new_internal);
        root = new_root;
        return;
    }

    // 부모에 다시 삽입 요청 (재귀적)
    InsertInternal(root, new_key, new_child, new_key);
    // To be implemented by students
}


// DeleteInternal function: Helper function to delete a key from an internal node.
template<typename Key>
bool Bplustree<Key>::DeleteInternal(Node* current, const Key& key) {
    // TODO: Implement internal node deletion logic here.
        if (current->is_leaf) {
        return false;  // 리프 노드는 여기에 올 수 없다.
    }

    InternalNode* internal_node = current->as_internal();
    auto it = std::lower_bound(internal_node->keys.begin(), internal_node->keys.end(), key);

    // 삭제하려는 키가 내부 노드에 없으면 리턴
    if (it == internal_node->keys.end() || *it != key) {
        return false;
    }

    // 삭제할 키 찾은 후 해당 자식 노드로 내려가기
    int index = std::distance(internal_node->keys.begin(), it);
    Node* child = internal_node->children[index];

    // 1. 자식 노드가 리프 노드인 경우, 자식에서 키 삭제
    if (child->is_leaf) {
        LeafNode* leaf_node = child->as_leaf();
        auto it_leaf = std::lower_bound(leaf_node->keys.begin(), leaf_node->keys.end(), key);
        leaf_node->keys.erase(it_leaf);  // 리프에서 키 삭제
    }

    // 2. 자식 노드가 내부 노드인 경우
    else {
        InternalNode* child_internal = child->as_internal();
        bool success = DeleteInternal(child, key);  // 자식 노드에서 재귀적으로 삭제

        // 자식 노드가 underflow 상태라면, 형제에서 키를 빌리거나 병합
        if (!success || child_internal->keys.size() < (degree - 1) / 2) {
            Node* sibling = nullptr;
            bool is_left_sibling = false;

            // 부모에서 형제 찾기
            if (index > 0) {
                sibling = internal_node->children[index - 1];
                is_left_sibling = true;
            } else if (index < internal_node->children.size() - 1) {
                sibling = internal_node->children[index + 1];
                is_left_sibling = false;
            }

            if (sibling) {
                InternalNode* sibling_internal = sibling->as_internal();

                // 형제에서 빌리기
                if (sibling_internal->keys.size() > (degree - 1) / 2) {
                    if (is_left_sibling) {
                        child_internal->keys.insert(child_internal->keys.begin(), internal_node->keys[index - 1]);
                        internal_node->keys[index - 1] = sibling_internal->keys.back();
                        sibling_internal->keys.pop_back();
                    } else {
                        child_internal->keys.push_back(internal_node->keys[index]);
                        internal_node->keys[index] = sibling_internal->keys.front();
                        sibling_internal->keys.erase(sibling_internal->keys.begin());
                    }
                }
                // 형제와 병합
                else {
                    if (is_left_sibling) {
                        sibling_internal->keys.push_back(internal_node->keys[index - 1]);
                        sibling_internal->keys.insert(sibling_internal->keys.end(), child_internal->keys.begin(), child_internal->keys.end());
                        internal_node->keys.erase(internal_node->keys.begin() + index - 1);
                        internal_node->children.erase(internal_node->children.begin() + index);
                    } else {
                        child_internal->keys.push_back(internal_node->keys[index]);
                        child_internal->keys.insert(child_internal->keys.end(), sibling_internal->keys.begin(), sibling_internal->keys.end());
                        internal_node->keys.erase(internal_node->keys.begin() + index);
                        internal_node->children.erase(internal_node->children.begin() + index + 1);
                    }
                }
            }
        }
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
        int idx = it - internal->keys.begin();
        current = internal->children[idx];
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