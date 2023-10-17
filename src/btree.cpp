/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "math.h"

//#define DEBUG

namespace badgerdb
{

	// -----------------------------------------------------------------------------
	// BTreeIndex::BTreeIndex -- Constructor
	// -----------------------------------------------------------------------------

	BTreeIndex::BTreeIndex(const std::string &relationName,
						   std::string &outIndexName,
						   BufMgr *bufMgrIn,
						   const int attrByteOffset,
						   const Datatype attrType)
	{
		// Creating index name
		std::cout << "Entered constructor\n";
		std::ostringstream indexString;
		indexString << relationName << '.' << attrByteOffset;
		outIndexName = indexString.str();
		// Initializing buffMgr
		bufMgr = bufMgrIn;
		// leafOccupancy = INTARRAYLEAFSIZE;
		// nodeOccupancy = INTARRAYNONLEAFSIZE;
		scanExecuting = false;
		// Try block to see if file exists
		try
		{
			std::cout << "Entered try block\n";
			// Throws FileNotFoundException if file doesn't exist
			file = new BlobFile(outIndexName, false);
			// Read metadata
			headerPageNum = file->getFirstPageNo();
			Page *header;
			bufMgr->readPage(file, 1, header);
			IndexMetaInfo *metadata = (IndexMetaInfo *)header;
			// Checking index metadata
			bool flag = false;
			if (relationName != metadata->relationName)
			{
				flag = true;
			}
			else if (attrType != metadata->attrType)
			{
				flag = true;
			}
			else if (attrByteOffset != metadata->attrByteOffset)
			{
				flag = true;
			}
			if (flag)
			{
				throw BadIndexInfoException(outIndexName);
			}
			// Assign rootPageNo
			rootPageNum = metadata->rootPageNo;
			// Unpin page from bufMgr
			std::cout << "unpin 1\n";
			bufMgr->unPinPage(file, 1, false);
		}
		// Catch block if file doesn't exist
		catch (FileNotFoundException e)
		{
			// This time, we call BlobFile with true as argument
			std::cout << "Entered catch\n";
			file = new BlobFile(outIndexName, true);
			// Allocate header and root pages
			Page *header, *root;
			bufMgr->allocPage(file, headerPageNum, header);
			bufMgr->allocPage(file, rootPageNum, root);
			// Copy metadata
			IndexMetaInfo *metadata = (IndexMetaInfo *)header;
			metadata->attrByteOffset = attrByteOffset;
			metadata->attrType = attrType;
			metadata->rootPageNo = rootPageNum;
			strncpy((char *)(&(metadata->relationName)), relationName.c_str(), 20);
			metadata->relationName[19] = 0;
			// TODO: Fix this part
			// Root node initialized with both leaf and non-leaf nodes
			// NonLeafNodeInt *rootNonLeaf = (NonLeafNodeInt *)root;
			// rootNonLeaf->level = 1;
			// LeafNodeInt *rootLeaf = new LeafNodeInt();
			PageId new_pid;
			Page *new_page;
			bufMgr->allocPage(file, new_pid, new_page);
			rootPageNum = new_pid;
			NonLeafNodeInt *rootNonLeaf = (NonLeafNodeInt *)new_page;
			rootNonLeaf->level = 1;
			for (int i = 0; i < INTARRAYNONLEAFSIZE; i++)
			{
				rootNonLeaf->keyArray[i] = -1;
				rootNonLeaf->pageNoArray[i] = (PageId)-1;
			}

			PageId new_leaf_pid;
			Page *new_leaf_page;
			bufMgr->allocPage(file, new_leaf_pid, new_leaf_page);
			LeafNodeInt *rootLeafNode = (LeafNodeInt *)new_leaf_page;
			rootLeafNode->rightSibPageNo = -1;
			for (int i = 0; i < INTARRAYLEAFSIZE; i++)
			{
				rootLeafNode->keyArray[i] = -1;
				rootLeafNode->ridArray[i].page_number = (PageId)-1;
				rootLeafNode->ridArray[i].slot_number = (PageId)-1;
			}

			// bufMgr->unPinPage(file, *rootLeaf, true);
			rootNonLeaf->pageNoArray[0] = new_leaf_pid;
			bufMgr->unPinPage(file, new_leaf_pid, true);
			bufMgr->unPinPage(file, new_pid, true);
			// Unpin header and root pages
			bufMgr->unPinPage(file, headerPageNum, true);
			// bufMgr->unPinPage(file, rootPageNum, true);
			// Using FileScan to fill the new file
			FileScan fScan(relationName, bufMgr);
			RecordId recID;
			std::cout << "Reached internal try\n";
			try
			{
				while (true)
				{
					fScan.scanNext(recID); // Throws EndOfFileException
					std::string record = fScan.getRecord();
					insertEntry(record.c_str() + attrByteOffset, recID);
				}
			}
			catch (EndOfFileException e)
			{
				// Save file from B+ Tree to disk
				bufMgr->flushFile(file);
			}
			delete &fScan;
			std::cout << "Finished constructor";
		}
	}

	// BTreeIndex::BTreeIndex(const std::string &relationName,
	// 					   std::string &outIndexName,
	// 					   BufMgr *bufMgrIn,
	// 					   const int attrByteOffset,
	// 					   const Datatype attrType)
	// {
	// 	// Constructing an index name
	// 	std::ostringstream idxStr;
	// 	idxStr << relationName << '.' << attrByteOffset;
	// 	outIndexName = idxStr.str(); // indeName is the name of the index file
	// 	// std::cout << "empty slot is" << -1 << std::endl; // output test for the the opened file
	// 	std::cout << "Name of index is: " << outIndexName << std::endl; // output test for the the opened file

	// 	// Declare a page instance
	// 	Page *page;

	// 	// Define the value of bufMgr
	// 	bufMgr = bufMgrIn;

	// 	// If the index file exists, the file is opened
	// 	// Check whether the file opened matches the buffer info
	// 	try
	// 	{

	// 		file = new BlobFile(outIndexName, false);
	// 		assert(file != NULL);
	// 		std::cout << "The file is opened." << std::endl; // output test for the the opened file

	// 		// Read the header page of from the buffer pool
	// 		bufMgr->readPage(file, 1, page);

	// 		// Cast page to IndexMetaInfo to retrieve info
	// 		IndexMetaInfo *indexMetaInfo = reinterpret_cast<IndexMetaInfo *>(page);

	// 		// Unpin header page since it is not required
	// 		bufMgr->unPinPage(file, 1, false);

	// 		bool flag = false;
	// 		if (relationName != indexMetaInfo->relationName)
	// 		{
	// 			flag = true;
	// 		}
	// 		else if (attrType != indexMetaInfo->attrType)
	// 		{
	// 			flag = true;
	// 		}
	// 		else if (attrByteOffset != indexMetaInfo->attrByteOffset)
	// 		{
	// 			flag = true;
	// 		}
	// 		if (flag)
	// 		{
	// 			throw BadIndexInfoException(outIndexName);
	// 		}

	// 		rootPageNum = indexMetaInfo->rootPageNo;
	// 	}

	// 	// If the index file does not exist, then a new file is created
	// 	catch (FileNotFoundException e)
	// 	{
	// 		// If the file does not exist, a new index file is created
	// 		file = new BlobFile(outIndexName, true);
	// 		std::cout << "A new index file is created"; // Test output message

	// 		Page *header, *root;
	// 		bufMgr->allocPage(file, headerPageNum, header);
	// 		bufMgr->allocPage(file, rootPageNum, root);
	// 		// Copy metadata
	// 		IndexMetaInfo *metadata = (IndexMetaInfo *)header;
	// 		PageId new_pid;
	// 		Page *new_page;
	// 		bufMgr->allocPage(file, new_pid, new_page);
	// 		rootPageNum = new_pid;
	// 		NonLeafNodeInt *rootNonLeaf = (NonLeafNodeInt *)new_page;
	// 		rootNonLeaf->level = 1;
	// 		for (int i = 0; i < INTARRAYNONLEAFSIZE; i++)
	// 		{
	// 			rootNonLeaf->keyArray[i] = -1;
	// 			rootNonLeaf->pageNoArray[i] = (PageId)-1;
	// 		}

	// 		PageId new_leaf_pid;
	// 		Page *new_leaf_page;
	// 		bufMgr->allocPage(file, new_leaf_pid, new_leaf_page);
	// 		LeafNodeInt *rootLeafNode = (LeafNodeInt *)new_leaf_page;
	// 		rootLeafNode->rightSibPageNo = -1;
	// 		for (int i = 0; i < INTARRAYLEAFSIZE; i++)
	// 		{
	// 			rootLeafNode->keyArray[i] = -1;
	// 			rootLeafNode->ridArray[i].page_number = (PageId)-1;
	// 			rootLeafNode->ridArray[i].slot_number = (PageId)-1;
	// 		}

	// 		// bufMgr->unPinPage(file, *rootLeaf, true);
	// 		rootNonLeaf->pageNoArray[0] = new_leaf_pid;
	// 		bufMgr->unPinPage(file, new_leaf_pid, true);
	// 		bufMgr->unPinPage(file, new_pid, true);

	// 		metadata->rootPageNo = rootPageNum;

	// 		bufMgr->unPinPage(file, new_pid, true);

	// 		// Scan the relationship(using FileScan)
	// 		FileScan *myFileScan = new FileScan(relationName, bufMgrIn);

	// 		// Scan the relationship using FileScan and insert the entries for all the tuples
	// 		// in this relation into the index
	// 		try
	// 		{
	// 			while (1)
	// 			{
	// 				RecordId outRid;
	// 				myFileScan->scanNext(outRid);

	// 				// Using getRecord() method to get all the record in the file
	// 				std::string record = myFileScan->getRecord();
	// 				const char *cstr = record.c_str();
	// 				// std::cout << "My record: " << record << std::endl;
	// 				// std::cout << outRid.page_number << "   " << outRid.slot_number << "\n";
	// 				insertEntry(cstr + attrByteOffset, outRid);
	// 			}
	// 		}
	// 		catch (EndOfFileException e)
	// 		{
	// 			std::cout << "Reach the end of the file." << std::endl;
	// 		}
	// 		delete myFileScan;
	// 	}
	// }

	// -----------------------------------------------------------------------------
	// BTreeIndex::~BTreeIndex -- destructor
	// -----------------------------------------------------------------------------

	BTreeIndex::~BTreeIndex()
	{
		scanExecuting = false;
		bufMgr->flushFile(file);
		delete file;
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::insertEntry
	// -----------------------------------------------------------------------------

	void BTreeIndex::insertEntry(const void *key, const RecordId rid)
	{
		std::cout << "Begun insert\n";
		PageKeyPair<int> root_changes;
		root_changes.set(-1, -1);
		// This method inserts a new entry into the index using the pair <key, rid>.
		root_changes = navigate(rootPageNum, key, rid);
		std::cout << "Returned to insertEntry\n";
		if (root_changes.key == -1 && root_changes.pageNo == -1)
		{
			return;
		}
		else
		{
			root_updation(root_changes);
		}
	}

	void BTreeIndex::root_updation(PageKeyPair<int> root_changes)
	{
		std::cout << "Entered root_update";
		// Allocate a new page for the new node
		PageId new_pid;
		Page *new_page;
		bufMgr->allocPage(file, new_pid, new_page);

		// Make a new leaf node for the page and initialising leaf variables
		NonLeafNodeInt *new_non_leaf = (NonLeafNodeInt *)new_page; // TODO:check
		new_non_leaf->level = 0;

		for (int i = 0; i < INTARRAYNONLEAFSIZE; i++)
		{
			new_non_leaf->keyArray[i] = -1;
			new_non_leaf->pageNoArray[i] = -1;
		}

		NonLeafNodeInt *newRoot = (NonLeafNodeInt *)(new_pid);
		newRoot->level = 0;
		newRoot->keyArray[0] = root_changes.key;
		newRoot->pageNoArray[0] = rootPageNum;
		newRoot->pageNoArray[1] = root_changes.pageNo;
		bufMgr->unPinPage(file, new_pid, true);

		rootPageNum = new_pid;

		IndexMetaInfo *meta;
		bufMgr->readPage(file, 1, (Page *&)meta);
		meta->rootPageNo = rootPageNum;
		bufMgr->unPinPage(file, 1, true);
	}

	PageKeyPair<int> BTreeIndex::navigate(const PageId pageNo, const void *key, const RecordId rid)
	{
		std::cout << "Entered navigate\n";
		// Make a new temporary node
		NonLeafNodeInt *this_node; // TODO: CHECK
		// Holder for changes recieved from leaf node
		PageKeyPair<int> changes;
		changes.set(-1, -1);

		bool dirty = false; // TODO: check

		// Read page into node
		bufMgr->readPage(file, pageNo, (Page *&)this_node);
		// Check if reached leaf
		if (this_node->level == 1)
			// Get next node PageNo and go to leaf level
			changes = go_to_leaf(get_next_node(this_node, key), key, rid);
		else
			// Get next node PageNo and recurse
			changes = navigate(get_next_node(this_node, key), key, rid);
		std::cout << "Returned to navigate\n";
		if (changes.key == -1 && changes.pageNo == -1)
		{
			bufMgr->unPinPage(file, pageNo, dirty);
			return changes;
		}
		if (this_node->keyArray[INTARRAYNONLEAFSIZE - 1] == -1)
		{
			// Insert changes returned to this level
			insert_in_non_leaf(this_node, changes);
			changes.set(-1, -1);
		}
		else
		{
			// Split this node and Insert changes returned to this level
			changes = split_non_leaf(this_node, changes);
		}
		bufMgr->unPinPage(file, pageNo, dirty);
		std::cout << "before navigate returns\n";
		return changes;
	}

	PageId BTreeIndex::get_next_node(NonLeafNodeInt *this_node, const void *key)
	{
		std::cout << "Entered get_next_node\n";
		for (int i = 0; i < INTARRAYNONLEAFSIZE; i++)
		{
			if (this_node->keyArray[i] >= *(int *)key)
				return this_node->pageNoArray[i];
			if (this_node->keyArray[i] == -1)
				return this_node->pageNoArray[i];
		}
	}

	PageKeyPair<int> BTreeIndex::go_to_leaf(const PageId pageNo, const void *key, const RecordId rid)
	{
		std::cout << "Entered go_to_leaf\n";
		LeafNodeInt *this_node; // TODO: CHECK
		// Holder for changes to send to Non leaf node
		PageKeyPair<int> copy_changes;
		copy_changes.set(-1, -1);

		bufMgr->readPage(file, pageNo, (Page *&)this_node);
		// If leaf is not full insert entry else split
		if (this_node->keyArray[INTARRAYLEAFSIZE - 1] == -1)
			insert_in_leaf(this_node, key, rid);
		else
			copy_changes = split_leaf(this_node, key, rid); // changes returned after split
		bool dirty = false;
		bufMgr->unPinPage(file, pageNo, dirty);

		// Sending changes up to the non leaf node
		std::cout << "copy_changes.key: " << copy_changes.key << std::endl;
		return copy_changes;
	}

	// TODO: complete
	void BTreeIndex::insert_in_non_leaf(NonLeafNodeInt *node, const PageKeyPair<int> changes)
	{
		std::cout << "Entered insert_in_non_leaf\n";
		std::cout << changes.key << std::endl;
		int index;
		for (int i = 0; i < INTARRAYLEAFSIZE; i++)
		{
			std::cout << "non_leaf for loop " << i << std::endl;
			if (node->keyArray[i] == -1)
			{
				std::cout << "Inside sus if statement\n";
				// node->keyArray[i] = *((int *)changes.key);
				std::cout << "if not sus\n";
				index = i;
				break;
			}
		}
		// Base case
		if (index == 0)
		{
			std::cout << "Base case b4\n";
			node->keyArray[0] = changes.key;
			node->pageNoArray[0] = changes.pageNo;
			std::cout << "Base case after\n";
		}
		else
		{
			// Find index
			std::cout << "Before weird index\n";
			for (index; index >= 0; index--)
			{
				if (node->keyArray[index - 1] > changes.key)
				{
					node->keyArray[index] = node->keyArray[index - 1];
					node->pageNoArray[index] = node->pageNoArray[index - 1];
				}
				else
					break;
			}
			std::cout << "after weird index\n";

			// Set value
			node->keyArray[index] = changes.key;
			node->pageNoArray[index] = changes.pageNo;
		}
		std::cout << "insert_in_non_leaf over\n";
	}

	// TODO: complete
	void BTreeIndex::insert_in_leaf(LeafNodeInt *node, const void *key, const RecordId rid)
	{
		std::cout << "Entered insert_in_leaf\n";
		int index;
		for (int i = 0; i < INTARRAYLEAFSIZE; i++)
		{
			if (node->keyArray[i] == -1)
			{
				node->keyArray[i] = *((int *)key);
				index = i;
				break;
			}
		}
		// Base case
		if (index == 0)
		{
			node->keyArray[0] = *((int *)key);
			node->ridArray[0] = rid;
		}
		else
		{
			// Find index
			for (index; index >= 0; index--)
			{
				if (node->keyArray[index - 1] > *((int *)key))
				{
					node->keyArray[index] = node->keyArray[index - 1];
					node->ridArray[index] = node->ridArray[index - 1];
				}
				else
					break;
			}

			// Set value
			node->keyArray[index] = *((int *)key);
			node->ridArray[index] = rid;
		}
	}

	PageKeyPair<int> BTreeIndex::split_leaf(LeafNodeInt *node, const void *key, const RecordId rid)
	{
		std::cout << "Entered split_leaf\n";
		// Get half size
		int half = ceil(INTARRAYLEAFSIZE / 2);
		// Allocate a new page for the new node

		PageId new_pid;
		Page *new_page;
		std::cout << "Before allocPage\n";
		bufMgr->allocPage(file, new_pid, new_page);
		std::cout << "After allocPage\n";
		// Make a new leaf node for the page and initialising leaf variables
		LeafNodeInt *new_leaf = (LeafNodeInt *)new_page; // TODO:check
		new_leaf->rightSibPageNo = -1;

		for (int i = 0; i < INTARRAYLEAFSIZE; i++)
		{
			new_leaf->keyArray[i] = -1;
			new_leaf->ridArray[i].page_number = -1;
			new_leaf->ridArray[i].slot_number = -1;
		}

		// Check if key goes to old leaf or new leaf
		bool key_to_left = false;
		if (*((int *)key) < node->keyArray[half])
		{
			key_to_left = true;
			half--;
		}

		// Copying values into new leaf and replacing them in existing leaf
		for (int i = half; i < INTARRAYLEAFSIZE; i++)
		{
			new_leaf->keyArray[i - half] = node->keyArray[i];
			new_leaf->ridArray[i - half] = node->ridArray[i];
			node->keyArray[i] = -1;
			node->ridArray[i].page_number = -1;
			node->ridArray[i].slot_number = -1;
		}

		// Insert new record
		if (key_to_left)
			insert_in_leaf(node, key, rid);
		else
			insert_in_leaf(new_leaf, key, rid);

		// Modifying sibling values
		new_leaf->rightSibPageNo = node->rightSibPageNo;
		node->rightSibPageNo = new_pid;

		// Unpin this page from buffer
		bufMgr->unPinPage(file, new_pid, new_page);

		// Send back the changes to the upper level node
		PageKeyPair<int> newPair;
		newPair.set(new_pid, new_leaf->keyArray[0]);
		return newPair;
	}

	PageKeyPair<int> BTreeIndex::split_non_leaf(NonLeafNodeInt *node, const PageKeyPair<int> changes)
	{
		std::cout << "Entered split_non_leaf";
		PageKeyPair<int> push_up_changes;
		// Get half size
		int half = ceil(INTARRAYLEAFSIZE / 2);
		// Allocate a new page for the new node
		PageId new_pid;
		Page *new_page;
		bufMgr->allocPage(file, new_pid, new_page);

		// Make a new leaf node for the page and initialising leaf variables
		NonLeafNodeInt *new_non_leaf = (NonLeafNodeInt *)new_page; // TODO:check
		new_non_leaf->level = 0;

		for (int i = 0; i < INTARRAYNONLEAFSIZE; i++)
		{
			new_non_leaf->keyArray[i] = -1;
			new_non_leaf->pageNoArray[i] = -1;
		}

		// Start changes TODO: complete
		int index = 0;
		for (; index < INTARRAYNONLEAFSIZE && node->keyArray[index] < changes.key; index++)
			;

		if (index < half)
		{
			for (int i = 0; i < half - 1; i++)
			{
				new_non_leaf->keyArray[i] = node->keyArray[i + half];
				new_non_leaf->pageNoArray[i] = node->pageNoArray[i + half];
				node->keyArray[i + half] = -1;
				node->pageNoArray[i + half] = -1;
			}
			new_non_leaf->pageNoArray[half - 1] = node->pageNoArray[INTARRAYNONLEAFSIZE];
			node->pageNoArray[INTARRAYNONLEAFSIZE] = -1;

			push_up_changes.set(new_pid, node->keyArray[half - 1]);

			node->keyArray[half - 1] = -1;
			insert_in_non_leaf(node, changes);
		}
		else if (index == half)
		{
			for (int i = 0; i < half - 1; i++)
			{
				new_non_leaf->keyArray[i] = node->keyArray[i + half];
				new_non_leaf->pageNoArray[i + 1] = node->pageNoArray[i + half + 1];
				node->keyArray[i + half] = -1;
				node->pageNoArray[i + half + 1] = -1;
			}
			new_non_leaf->pageNoArray[0] = new_pid;

			push_up_changes.set(new_pid, changes.key);
		}
		else
		{
			for (int i = 0; i < half - 2; i++)
			{
				new_non_leaf->keyArray[i] = node->keyArray[i + half + 1];
				new_non_leaf->pageNoArray[i] = node->pageNoArray[i + half + 1];
				node->keyArray[i + half + 1] = -1;
				node->pageNoArray[i + half + 1] = -1;
			}
			new_non_leaf->pageNoArray[half - 2] = node->pageNoArray[INTARRAYNONLEAFSIZE];
			node->pageNoArray[INTARRAYNONLEAFSIZE] = -1;

			new_non_leaf->level = node->level;

			// TODO: above stuff optimize
		}
		// end
		bool dirty = false;
		bufMgr->unPinPage(file, new_pid, dirty);
		return push_up_changes;
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::startScan
	// -----------------------------------------------------------------------------
	/**
	 * Begin a filtered scan of the index.  For instance, if the method is called
	 * using ("a",GT,"d",LTE) then we should seek all entries with a value
	 * greater than "a" and less than or equal to "d".
	 * If another scan is already executing, that needs to be ended here.
	 * Set up all the variables for scan. Start from root to find out the leaf page that contains the first RecordID
	 * that satisfies the scan parameters. Keep that page pinned in the buffer pool.
	 * @param lowValParm	Low value of range, pointer to integer / double / char string
	 * @param lowOpParm		Low operator (GT/GTE)
	 * @param highValParm	High value of range, pointer to integer / double / char string
	 * @param highOpParm	High operator (LT/LTE)
	 * @throws  BadOpcodesException If lowOp and highOp do not contain one of their their expected values
	 * @throws  BadScanrangeException If lowVal > highval
	 * @throws  NoSuchKeyFoundException If there is no key in the B+ tree that satisfies the scan criteria.
	 **/
	void BTreeIndex::startScan(const void *lowValParm,
							   const Operator lowOpParm,
							   const void *highValParm,
							   const Operator highOpParm)
	{
		lowValInt = *((int *)lowValParm);
		lowOp = lowOpParm;
		highValInt = *((int *)highValParm);
		highOp = highOpParm;
		if (lowOp != GT && lowOp != GTE)
		{
			throw BadOpcodesException();
		}
		if (highOp != LT && highOp != LTE)
		{
			throw BadOpcodesException();
		}
		if (lowValInt > highValInt)
		{
			throw BadScanrangeException();
		}
		PageId pageNo = rootPageNum;
		NonLeafNodeInt *node;
		LeafNodeInt *leaf;

		bufMgr->readPage(file, pageNo, (Page *&)node);
		while (node->level == 0)
		{
			int i;
			for (i = 0; i < INTARRAYNONLEAFSIZE; i++)
			{
				if (node->keyArray[i] >= lowValInt)
				{
					break;
				}
				if (node->keyArray[i] == -1)
				{
					break;
				}
			}
			PageId oldPageNo = pageNo;
			pageNo = node->pageNoArray[i];
			bufMgr->unPinPage(file, oldPageNo, false);
			bufMgr->readPage(file, pageNo, (Page *&)node);
		}
		{
			int i;
			for (i = 0; i < INTARRAYNONLEAFSIZE; i++)
			{
				if (node->keyArray[i] >= lowValInt)
				{
					break;
				}
				if (node->keyArray[i] == -1)
				{
					break;
				}
			}
			PageId oldPageNo = pageNo;
			pageNo = node->pageNoArray[i];
			bufMgr->unPinPage(file, oldPageNo, false);
			bufMgr->readPage(file, pageNo, (Page *&)leaf);
		}

		bool ans = false;
		for (int i = 0; i < INTARRAYLEAFSIZE && leaf->keyArray[i] != -1; i++)
		{
			int key = leaf->keyArray[i];
			bool conditions = false;
			if (lowOp == GTE && highOp == LTE)
			{
				conditions = key >= lowValInt && key <= highValInt;
			}
			else if (lowOp == GT && highOp == LTE)
			{
				conditions = key > lowValInt && key <= highValInt;
			}
			else if (lowOp == GTE && highOp == LT)
			{
				conditions = key >= lowValInt && key < highValInt;
			}
			else
			{
				conditions = key > lowValInt && key < highValInt;
			}
			if (conditions)
			{
				ans = true;
				scanExecuting = true;
				currentPageNum = pageNo;
				currentPageData = (Page *)leaf;
				nextEntry = i;
				break;
			}
		}

		if (!ans)
		{
			scanExecuting = false;
			bufMgr->unPinPage(file, pageNo, false);
			throw NoSuchKeyFoundException();
		}
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::scanNext
	// -----------------------------------------------------------------------------

	void BTreeIndex::scanNext(RecordId &outRid)
	{
		if (!scanExecuting)
		{
			throw ScanNotInitializedException();
		}

		LeafNodeInt *node = (LeafNodeInt *)currentPageData;

		if (nextEntry == INTARRAYLEAFSIZE || node->keyArray[nextEntry] == -1)
		{
			if (node->rightSibPageNo == (PageId)-1)
			{
				throw IndexScanCompletedException();
			}
			const PageId oldPageNum = currentPageNum;
			currentPageNum = node->rightSibPageNo;
			nextEntry = 0;
			bufMgr->unPinPage(file, oldPageNum, false);
			bufMgr->readPage(file, currentPageNum, (Page *&)currentPageData);
			node = (LeafNodeInt *)currentPageData;
		}

		const int key = node->keyArray[nextEntry];

		bool check = false;
		if (lowOp == GTE && highOp == LTE)
		{
			check = key >= lowValInt && key <= highValInt;
		}
		else if (lowOp == GT && highOp == LTE)
		{
			check = key > lowValInt && key <= highValInt;
		}
		else if (lowOp == GTE && highOp == LT)
		{
			check = key >= lowValInt && key < highValInt;
		}
		else
		{
			check = key > lowValInt && key < highValInt;
		}

		if (check)
		{
			outRid = node->ridArray[nextEntry];
			nextEntry++;
		}
		else
		{
			throw IndexScanCompletedException();
		}
	}

	// -----------------------------------------------------------------------------
	// BTreeIndex::endScan
	// -----------------------------------------------------------------------------
	//
	void BTreeIndex::endScan()
	{
		if (!scanExecuting)
		{
			throw ScanNotInitializedException();
		}
		bufMgr->unPinPage(file, currentPageNum, false);

		scanExecuting = false;
		currentPageData = nullptr;
		currentPageNum = -1;
		nextEntry = -1;
	}
}
