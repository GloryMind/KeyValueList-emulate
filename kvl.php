<?php

namespace Reader;

class Stat {
	private $index = 0;
	private $array = [];
	public function __construct( $size = 1024 ) {
		while($size--) {
			$this->array[] = [ 1234 , 1.1 ];
		}
	}
	
	public function Add() {
		$this->array[ $this->index ][0] = memory_get_usage();
		$this->array[ $this->index ][1] = microtime(1);
		$this->index++;
	}
	public function Show() {
		$buf = "";
		foreach($this->array as $i => $info) {
			if ( $i === 0 ) { continue; }
			if ( $i >= $this->index ) { break; }
			$buf .= 
				"Add ~ Memory: " . $this->ViewMemory($this->array[ $i ][0] - $this->array[ $i - 1 ][0]) .
				"; Time: " . ($this->array[ $i ][1] - $this->array[ $i - 1 ][1]) . 
				"\r\n";
		}
		return $buf;
	}
	
	private function ViewMemory( $mem ) {
		$buf = "";
		foreach([30 => 'gb', 20 => 'mb', 10 => 'kb'] as $shr => $s) {
			if ( $_ = ($mem >> $shr) & 1023 ) {
				$buf .= $_ . $s . " ";
			}
		}
		if ( $_ = $mem & 1023 ) {
			$buf .= $_ . "b";
		}
		return trim($buf);
	}
}

trait CommonAvlNode {
	public function GetHeight() {
		return $this->OffsetOf() ? $this->height : 0;
	}
	public function GetDeltaHeight() {
		return $this->right()->GetHeight() - $this->left()->GetHeight();
	}
	public function SetHeight() {
		$this->height = max( $this->left()->GetHeight() , $this->right()->GetHeight() ) + 1;
	}
	public function ToRight() {
		$tmp = $this->left();
		$this->left = $tmp->right;
		$tmp->right = $this->OffsetOf();
		
		$this->SetHeight();
		$tmp->SetHeight();
		return $tmp->OffsetOf();
	}
	public function ToLeft() {
		$tmp = $this->right();
		$this->right = $tmp->left;
		$tmp->left = $this->OffsetOf();
		
		$this->SetHeight();
		$tmp->SetHeight();
		return $tmp->OffsetOf();
	}
	public function Balance() {
		$this->SetHeight();

		$bfactorNode = $this->GetDeltaHeight();
		if( $bfactorNode == 2 ) {
			if ( $this->right()->GetDeltaHeight() < 0 ) {
				$this->right = $this->right()->ToRight();
			}
			return $this->ToLeft();
		}
		if( $bfactorNode == -2 ) {
			if( $this->left()->GetDeltaHeight() > 0 ) {
				$this->left = $this->left()->ToLeft();
			}
			return $this->ToRight();
		}
		return $this->OffsetOf();
	}
	public function FindMin() {
		return $this->left ? $this->left()->Findmin() : $this;
	}
	public function RemoveMin() {
		if ( !$this->left )
			return $this->right;
		$this->left = $this->left()->RemoveMin();
		return $this->Balance();
	}
}

class FileManager {
	private $path;
	private $handle;
	private $offset;
	public function __construct( $path ) {
		$this->path = $path;
	}
	
	public function Open() {
		if ( false === $this->handle = @fopen($this->path , "c+") ) {
			$this->riseError( "Fopen error: '{$this->path}'" );
		}
		return $this;
	}
	
	private $is_lock = false;
	public function Lock( $wait = true ) {
		if ( $this->is_lock ) { return $this; }
		if ( !flock($this->handle , LOCK_EX|($wait?0:LOCK_UN)) ) {
			if ( !$wait ) { return $this; }
			$this->riseError( "Flock error{LOCK_EX}" );
		}
		$this->is_lock = true;
		return $this;
	}
	public function UnLock() {
		if ( !$this->is_lock ) { return $this; }
		if ( !flock($this->handle , LOCK_UN) ) {
			$this->riseError( "Flock error{LOCK_UN}" );
		}
		
		$this->is_lock = false;
		return $this;
	}
	public function IsLock() {
		if ( $this->is_lock ) { return true; }
		$this->Lock(false);
		if ( !$this->is_lock ) { return false; }
		$this->UnLock();
		return true;
	}
	public function Read($offset = null, $size = null) {
		if ( $size === null ) {
			$size = $offset;
			$offset = $this->GetPosition();
		}
		if ( $size === null ) {
			$size = $this->GetSize() - $offset;
		}
		$this->SetPosition( $offset );
		if ( false === $buf = fread($this->handle, $size) ) {
			$this->riseError("Fread error");
		}
		$this->AddPosition( strlen($buf) );
		return $buf;
	}
	public function Write($offset, $data = null) {
		if ( $data === null ) {
			$data = $offset;
			$offset = $this->GetPosition();
		}
		$this->SetPosition( $offset );
		if ( fwrite($this->handle, $data) !== strlen($data) ) {
			$this->riseError("Fwrite error(data length ".strlen($data).")");
		}
		$this->SetPosition( $offset + strlen($data) );
		
		return $this;
	}
	public function GetSize() {
		if ( !( $stat = @fstat($this->handle) ) ) {
			$this->riseError("Fstat error");
		}
		return $stat['size'];
	}
	public function SetSize( $size = 0 ) {
		if ( ftruncate($this->handle, $size ) === false ) {
			$this->riseError("Ftruncate error");
		}
		return $this;
	}
	public function GetPosition() {
		if ( false === $pos = ftell($this->handle) ) {
			$this->riseError("Ftell error");
		}
		return $pos;
	}
	public function SetPosition( $offset ) {
		if ( @fseek($this->handle, $offset) !== 0 ) {
			$this->riseError("Fseek {$offset} error");
		}
		return $this;
	}
	public function AddPosition( $offset ) { $this->SetPosition( $this->GetPosition() + $offset ); return $this; }
	public function DddPosition( $offset ) { $this->SetPosition( $this->GetPosition() - $offset ); return $this; }
	public function Close() {
		if ( $this->handle ) {
			if ( @fclose($this->handle) === false ) {
				$this->riseError("Fclose error");
			}
			$this->handle = null;
		}
		return $this;
	}

	private $riseErrors = [];
	private function riseError( $text , $closeFile = true ) {
		static $recLv = 0;
		$recLv++;

		$this->riseErrors[] = "File error.\r\n{$text}\r\nPath: '{$this->path}\r\n";
		
		if ( $closeFile ) { $this->Close(); }

		if ( $recLv === 1 ) {
			$recLv = 0;
			throw new \Exception( implode('',$this->riseErrors) );
		}
		
		$recLv--;
	}
	
	public function GetHandle() {
		return $this->handle;
	}
}

//**********************************
interface MemoryRawIO {
	public function GetSize();
	public function IncreaseSize( $size );
	public function ReduceSize( $size );
	public function ReadBuffer( $offset, $size );
	public function WriteBuffer( $offset, $data );
}

//**********************************
interface MemoryReaderIO {
	public function ReadInt8($offset);
	public function ReadInt16($offset);
	public function ReadInt32($offset);
	
	public function WriteInt8($offset, $data);
	public function WriteInt16($offset, $data);
	public function WriteInt32($offset, $data);
	
	public function ReadUInt8($offset);
	public function ReadUInt16($offset);
	public function ReadUInt32($offset);
	
	public function WriteUInt8($offset, $data);
	public function WriteUInt16($offset, $data);
	public function WriteUInt32($offset, $data);

	public function ReadBuffer($offset, $size);
	public function WriteBuffer($offset, $data);
}

//**********************************
interface MemoryManagerIO {
	public function Allocate( $size );
	public function Free( $ptr );
}


//**********************************
//**********************************
//**********************************

//**********************************
class File_MemoryRawIO implements MemoryRawIO {
	private $mn;
	private $size;
	
	public function __construct( FileManager $mn ) {
		$this->mn = $mn;
		$this->size = $this->mn->GetSize();
	}
	
	
	public function GetSize() {
		return $this->size;
	}
	public function IncreaseSize( $size ) {
		$offset = $this->size;
		$this->size += $size;
		$this->mn->SetSize( $this->size );
		$this->WriteBuffer($offset , str_repeat("\0", $size) );
		return $offset;
	}
	public function ReduceSize( $size ) {
		$this->size -= $size;
		if ( $this->size < 0 ) {
			$this->size = 0;
		}
		$this->mn->SetSize( $this->size );
	}


	public function ReadBuffer( $offset, $size ) {
		$this->TryOffsetSize($offset, $size);
		return $this->mn->Read($offset, $size);
	}
	public function WriteBuffer( $offset, $data ) {
		$this->TryOffsetSize($offset, strlen($data));
		return $this->mn->Write($offset, $data);
	}


	private function TryOffsetSize($offset, $size) {
		if ( $offset < 0 ) {
			throw new \Exception("Offset: {$offset} < 0");
		}
		if ( $offset + $size > $this->size ) {
			throw new \Exception("Offset + size > real size: {$offset} + {$size} > {$this->size}");
		}
	}
}

//**********************************
class MemoryReader implements MemoryReaderIO {
	private $memory;
	private $x32;
	private $x64;
	private $_8;
	private $_16;
	private $_24;
	private $_32;
	private $_64;
	private $_i8;
	private $_i16;
	private $_i24;
	private $_i32;
	private $_i64;

	public function __construct( MemoryRawIO $memory ) {
		$this->memory = $memory;
		
		$this->x64 = !$this->x32 = is_float(4294967297);
		foreach([8,16,24,32,/*64*/] as $bits) {
			$this->{"_{$bits}"} = 1 << ($bits-1);
		}
		foreach([8,16,24,32,/*64*/] as $bits) {
			$this->{"_i{$bits}"} = -1 >> $bits << $bits;
		}
		if ( $this->x32 ) { $this->_i32 = 0; }
		if ( $this->x64 ) { $this->_i64 = 0; }
	}


	public function ReadUInt8($offset) {
		$bin = $this->memory->ReadBuffer($offset,1);
		return ord($bin[0]);
	}
	public function ReadUInt16($offset) {
		$bin = $this->memory->ReadBuffer($offset,2);
		return (ord($bin[0])) | (ord($bin[1]) <<  8);
	}
	public function ReadUInt32($offset) {
		$bin = $this->memory->ReadBuffer($offset,4);
		return (ord($bin[0])) | (ord($bin[1]) <<  8) | (ord($bin[2]) << 16) | (ord($bin[3]) << 24);
	}

	public function WriteUInt8($offset, $data) {
		$this->memory->WriteBuffer($offset, chr($data));
	}
	public function WriteUInt16($offset, $data) {
		$this->memory->WriteBuffer($offset, chr($data) . chr($data >>  8));
	}
	public function WriteUInt32($offset, $data) {
		$this->memory->WriteBuffer($offset, chr($data) . chr($data >>  8) . chr($data >> 16) . chr($data >> 24) );
	}

	
	public function ReadInt8($offset) {
		$num = $this->ReadUInt8($offset);
		if ( $num & $this->_8 ) { $num |= ($this->_i8); }
		return $num;
	}
	public function ReadInt16($offset) {
		$num = $this->ReadUInt16($offset);
		if ( $num & $this->_16 ) { $num |= ($this->_i16); }
		return $num;
	}
	public function ReadInt32($offset) {
		$num = $this->ReadUInt32($offset);
		if ( $num & $this->_32 ) { $num |= ($this->_i32); }
		return $num;
	}
	
	public function WriteInt8($offset, $data) {
		if ( $data < 0 ) { $data |= $this->_8; }
		return $this->WriteUInt8($offset, $data);
	}
	public function WriteInt16($offset, $data) {
		if ( $data < 0 ) { $data |= $this->_16; }
		return $this->WriteUInt16($offset, $data);
	}
	public function WriteInt32($offset, $data) {
		if ( $data < 0 ) { $data |= $this->_32; }
		return $this->WriteUInt32($offset, $data);
	}

	
	public function ReadBuffer($offset, $size) {
		return $this->memory->ReadBuffer($offset, $size);
	}
	public function WriteBuffer($offset, $data) {
		$this->memory->WriteBuffer($offset, $data);
	}
}

//**********************************
class _AllocateMemoryStructure extends MemoryStructure {
	public $memoryBlockLast = "*_MemoryBlockMemoryStructure";
	public $avl = "_MemoryAvlTreeMemoryStructure";
}
class _MemoryBlockMemoryStructure extends MemoryStructure {
	public $prev = "*_MemoryBlockMemoryStructure";
	public $isAlive = "uint32";
	public $size = "uint32";
	public $dataOffset = "<";
	public function MemSet($c = "\0") {
		$this->GetIO()->WriteBuffer($this->dataOffset, str_repeat($c, $this->size));
		return $this;
	}
	public function GetAllSize() {
		return $this->size + ($this->dataOffset - $this->OffsetOf());
	}
	public function SetSizeByAllSize( $allSize ) {
		$this->size = $allSize - ($this->dataOffset - $this->OffsetOf());
	}
	public function GetNext() {
		return new _MemoryBlockMemoryStructure($this->GetIO(), $this->GetAllSize() + $this->OffsetOf());
	}
}
class _MemoryAvlNodeMemoryStructure extends MemoryStructure {
	public $_prev = "*_MemoryBlockFreeMemoryStructure";
	public $isAlive = "uint32";
	public $key = "uint32";
	public $left = "*_MemoryAvlNodeMemoryStructure";
	public $right = "*_MemoryAvlNodeMemoryStructure";
	public $height = "uint32";
	public $prev = "*_MemoryAvlNodeMemoryStructure";
	public $next = "*_MemoryAvlNodeMemoryStructure";

	use CommonAvlNode;

	public function Remove($key) {
		if( $key < $this->key ) {
			if ( !$this->left ) { return $this->OffsetOf(); }
			$this->left = $this->left()->Remove($key);
		} elseif( $key > $this->key ) {
			if ( !$this->right ) { return $this->OffsetOf(); }
			$this->right = $this->right()->Remove($key);
		} else {
			$q = $this->left;
			$r = $this->right;
			if ( !$r ) 
				return $q;
			$min = $this->right()->Findmin();
			$min->right = $this->right()->Removemin();
			$min->left = $q;
			return $min->Balance();
		}
		return $this->Balance();
	}
	
	public function Insert(_MemoryAvlNodeMemoryStructure $node) {
		$key = $node->key;
		if ( $key < $this->key ) {
			$this->left = $this->left ? $this->left()->Insert($node) : $node->OffsetOf();
		} elseif ( $key > $this->key ) {
			$this->right = $this->right ? $this->right()->Insert($node) : $node->OffsetOf();
		} else {
			$node->next = $this->next;
			$this->next = $node->OffsetOf();
		}
		
		return $this->Balance();
	}
	

}
class _MemoryAvlTreeMemoryStructure extends MemoryStructure {
	public $node = "*_MemoryAvlNodeMemoryStructure";
	
	public function Add(_MemoryAvlNodeMemoryStructure $node) {
		$node->left = 0;
		$node->right = 0;
		$node->height = 0;
		$node->prev = 0;
		$node->next = 0;
		if ( !$this->node ) {
			$this->node = $node->OffsetOf();
			return;
		}
		$this->node = $this->node()->Insert( $node );
	}
	public function Search($key, $type = "=") {
		if ( !$this->node ) { return; }
		$node = $this->node();
		$key = (int)$key;
		switch($type) {
			case "=":
				while(1) {
					if ( $key < $node->key ) {
						if ( !$node->left ) { return; }
						$node = $node->left(); 
						continue; 
					}
					if ( $key > $node->key ) { 
						if ( !$node->right ) { return; }
						$node = $node->right(); 
						continue; 
					}
					return $node;
				}
			case "<": $key--;
			case "<=":
				while(1) {
					if ( $node->key <= $key ) { return $node; }
					if ( !$node->left ) { return; }
					$node = $node->left();
				}
				return;
			case ">":	$key++;
			case ">=":
				while(1) {
					if ( $node->key >= $key ) { return $node; }
					if ( !$node->right ) { return; }
					$node = $node->right();
				}
				return;
		}
	}
	public function Remove($key) {
		if ( !$this->node ) { return; }
		$key = (int)$key;
		$this->node = $this->node()->Remove($key);
	}	
}

class MemoryManager implements MemoryManagerIO {
	private $memory;
	private $reader;
	
	private $allocate;
	private $memoryBlock;
	private $memoryBlockFree;
	
	private $sizeSetFreeMask;
	private $sizeIsFreeMask;
	
	const ALIVE = 1;
	const FREE = 2;
	const FREE_AVL = 4;
	
	public function __construct( MemoryRawIO $memory , MemoryReaderIO $reader ) {
		$this->sizeSetFreeMask = 1<<30;
		$this->sizeIsFreeMask = ~$this->sizeSetFreeMask;
		
		$this->memory = $memory;
		$this->reader = $reader;
		
		$this->allocate = new _AllocateMemoryStructure( $this->reader, 0 );
		$this->memoryBlock = new _MemoryBlockMemoryStructure( $this->reader, 0 );
		$this->memoryBlockFree = new _MemoryAvlNodeMemoryStructure( $this->reader, 0 );
		$this->init();
	}
	
	private function init() {
		if ( $this->memory->GetSize() < $this->allocate->SizeOf() ) {
			$this->memory->ReduceSize( 0 );
			$this->memory->IncreaseSize( $this->allocate->SizeOf() );
			$this->memory->WriteBuffer( 0 , str_repeat("\0", $this->allocate->SizeOf()) );
		}
		$this->allocate->SetOffset( 0 );
	}
	
	public function Allocate( $size ) {
		$size = (int) $size;
		if ( $size < 1 || $size >= 1 << 26 ) { throw new \Exception("Allocate error size: {$size}, need [1..".((1 << 26)-1)."]" ); }

		$size = (!$size || $size&3) ? 4 + $size & ~3 : $size;
		if ( $size < $this->memoryBlockFree->SizeOf() - $this->memoryBlock->SizeOf() ) {
			$size = $this->memoryBlockFree->SizeOf() - $this->memoryBlock->SizeOf();
		}

		$comSize = $size + $this->memoryBlock->SizeOf();
//echo "Allocated :: started / size: {$size}\n";
		
		if ( $node = $this->allocate->avl->Search( $size , ">=" ) ) {
			$this->memoryBlock->SetOffset( $node->OffsetOf() );
			$this->NodeRemove( $node );
			
			$this->memoryBlock->isAlive = 1;
		
			$sizeDelta = $this->memoryBlock->size - $size;
			if ( $sizeDelta >= $this->memoryBlockFree->SizeOf() ) {
//echo "Allocated :: size dd / size: {$size}\n";
				$this->memoryBlock->size = $size;
				$this->SetMemoryToFree($this->memoryBlock->GetNext()->OffsetOf(), $sizeDelta);
			}
			
			$this->memoryBlock->MemSet();
			return $this->memoryBlock->dataOffset;
		}
		
//echo "	Allocated from heap\n";

		$this->memoryBlock->SetOffset( 
			$this->memory->IncreaseSize( $comSize )
		);
		$this->memoryBlock->isAlive = 1;
		$this->memoryBlock->size = $size;
		$this->memoryBlock->prev = $this->allocate->memoryBlockLast;
		$this->allocate->memoryBlockLast = $this->memoryBlock->OffsetOf();
		$this->memoryBlock->MemSet();
		
		return $this->memoryBlock->dataOffset;
	}
	public function Free( $ptr ) {
//echo "Free block :: started\n";
		$this->memoryBlock->SetOffset( $ptr - $this->memoryBlock->SizeOf() );
		$this->SetMemoryToFree( $this->memoryBlock->OffsetOf() , $this->memoryBlock->GetAllSize() );
	}
	
	public function NodeAdd( $node ) {
		$this->allocate->avl->Add( new _MemoryAvlNodeMemoryStructure($this->reader, $node->OffsetOf()) );
	}
	public function NodeRemove( $node ) {
		$node = new _MemoryAvlNodeMemoryStructure($this->reader, $node->OffsetOf());
		
		if ( !$node->prev ) {
			$this->allocate->avl->Remove( $node->key );
		} else {
			if ( $node->next ) {
				$node->next()->prev = $node->prev;
			}
			$node->prev()->next = $node->next;
		}
	}
	
	private function SetMemoryToFree( $offset, $size ) {
		$memBlock = new _MemoryBlockMemoryStructure($this->reader, $offset);
		$memBlock->isAlive = 0;
		$memBlock->SetSizeByAllSize( $size );
		if ( $memBlock->GetNext()->OffsetOf() < $this->memory->GetSize() ) {
			$nextMemBlock = $memBlock->GetNext();
			if ( $nextMemBlock->prev !== $offset ) {
				$memBlock->prev = $nextMemBlock->prev;
				$nextMemBlock->prev = $offset;
			}
		}
		
		while( $memBlock->prev && ($prevMemBlock = $memBlock->prev()) && !$memBlock->prev()->isAlive ) {
			$this->NodeRemove( $prevMemBlock );
			$prevMemBlock->SetSizeByAllSize( $prevMemBlock->GetAllSize() + $memBlock->GetAllSize() );
			$memBlock = $prevMemBlock;
//echo "	?Free: Add prev block, Com size: ",$memBlock->size,"\n";
		}
		while( ($nextMemBlock = $memBlock->GetNext()) && ($nextMemBlock->OffsetOf() < $this->memory->GetSize()) && !$nextMemBlock->isAlive ) {
			$this->NodeRemove( $nextMemBlock );
			$memBlock->SetSizeByAllSize( $memBlock->GetAllSize() + $nextMemBlock->GetAllSize() );
//echo "	?Free: Add next block, Com size: ",$memBlock->size,"\n";
		}
		
		if ( $memBlock->GetNext()->OffsetOf() === $this->memory->GetSize() ) {
			$this->allocate->memoryBlockLast = $memBlock->prev;
			$this->memory->ReduceSize( $memBlock->GetAllSize() );
			return;
		}
		
		if ( $memBlock->GetNext()->OffsetOf() < $this->memory->GetSize() ) {
			$memBlock->GetNext()->prev = $memBlock->OffsetOf();
		}
		
		$this->NodeAdd( $memBlock );
	}

	public function pvTest($wCount = 10, $allocateRange = [1, 200]) {
		set_time_limit(0);
		$dataList = [];
		for($w=0; $w<$wCount; $w++) {
			$allocateCount = 0;
			for($i=0, $c=mt_rand($allocateRange[0],$allocateRange[1]); $i<$c; $i++) {
				$data = ''; for($G=0, $__c=mt_rand(1,10); $G<$__c; $G++) { $data .= md5(mt_rand(1,999999)); }
				
				$ofs = $this->Allocate( strlen($data) );
				
				$this->reader->WriteBuffer($ofs, $data);
				
				$dataList[] = [ $ofs, $data, $i ];

				$allocateCount++;
			}

			shuffle($dataList);

			$freeCount = 0;
			for($i=0, $c=mt_rand(0, count($dataList)); $i<$c; $i++) {
				reset($dataList);
				list($ofs,$data,$pos) = current($dataList);
				unset($dataList[key($dataList)]);

				echo "Size: ",strlen($data)," Offset: ",$ofs," Position: $pos\n";
				if ($this->reader->ReadBuffer($ofs, strlen($data)) !== $data) {
					echo "	Error: ",$ofs,"\n";
				}
				$this->Free($ofs);
				$v['alive'] = false;
				
				$freeCount++;
			}

			echo "AllocateCount: $allocateCount\n";
			echo "FreeCount: $freeCount\n-------\n";
		}
		$freeCount = 0;
		foreach($dataList as $v) {
			list($ofs,$data,$pos) = $v;
			$this->Free($ofs);
			$freeCount++;
		}
		echo "FreeCount: $freeCount\n-------\n";
	}
}

//**********************************
//**********************************
//**********************************

//**********************************
abstract class Memory {
	private $io;
	private $offset = 0;
	private $size = 0;
	private $type;

	public function SetIO( MemoryReaderIO $io ) {
		$this->io = $io;
	}
	public function SetOffset( $offset ) {
		$this->offset = $offset;
		return $this;
	}
	public function SetSize( $size ) {
		$this->size = $size;
		return $this;
	}

	public function GetIO() {
		return $this->io;
	}
	public function OffsetOf() {
		return $this->offset;
	}
	public function SizeOf() {
		return $this->size;
	}

	public function GetFullNameClass( $class ) {
		$class = trim( $class , "\\" );
		if ( class_exists( $_class = __NAMESPACE__ . "\\" . $class ) ) {
			return $_class;
		}
		if ( class_exists( $_class = $class ) ) {
			return $_class;
		}
		return false;
	}
	
	public function ParseTypeRaw($typeRaw) {
		$typeRaw = strtolower(trim($typeRaw));
		if ( preg_match("~^\*(.*)~", $typeRaw, $m) ) {
			if ( !strlen($nextTypeRaw = trim($m[1])) ) { $this->RiseError( "Expected next type, unxpected '*'" ); }
			return new MemoryPointer($this->GetIO(), $this->OffsetOf() + $this->SizeOf(), ['item' => $nextTypeRaw]);
		} elseif ( preg_match("~^\[(.*?)\](.*)~", $typeRaw, $m) ) {
			if ( !strlen($nextTypeRaw = trim($m[2])) ) { $this->RiseError( "Expected next type, unxpected '{$m[1]}'" ); }			
			$arrayCount = 0;
			eval( '$arrayCount=('.$m[1].');' );
			return new MemoryArray($this->GetIO(), $this->OffsetOf() + $this->SizeOf(), ['item' => $nextTypeRaw, "count" => $arrayCount]);
		} elseif ( preg_match("~^<(.*)~", $typeRaw, $m) ) {
			if ( strlen($nextTypeRaw = trim($m[1])) ) { $this->RiseError( "Unxpected '{$nextTypeRaw}'" ); }
			return new MemoryOffset($this->GetIO(), $this->OffsetOf() + $this->SizeOf());
		} elseif ( $_typeRaw = $this->GetFullNameClass($typeRaw) ) {
			return new $_typeRaw($this->GetIO(), $this->OffsetOf() + $this->SizeOf());
		} else {
			$this->RiseError( "Unxpected {$typeRaw}" );
		}
	}	
	
	public function RiseError( $text ) {
		throw new \Exception($text);
	}
	
	public function __construct(MemoryReaderIO $io, $offset = 0, $params = []) {
		$this->SetIO($io);
		$this->SetOffset($offset);
		if ( is_callable([$this,"onConstruct"]) ) {
			$this->onConstruct( $params );
		}
	}
}

class MemoryBase extends Memory {
	public function Get() {
		return $this->GetIO()->{"Read".$this->method}( $this->OffsetOf() );
	}
	public function Set( $value ) {
		$this->GetIO()->{"Write".$this->method}( $this->OffsetOf() , $value );
		return $this;
	}

	private $method;
	public function onConstruct() {
		$l = explode("\\", get_class($this));
		$this->method = $l[ count($l)-1 ];
		if ( preg_match("~\d*$~", $this->method, $m) ) {
			$this->SetSize($m[0] >> 3);
		}
	}
	
	public function __get($name) {
		$name = strtolower($name);
		return $this->Get();
	}
	public function __set($name, $value) {
		$name = strtolower($name);
		$this->Set($value);
		return $this;
	}
}
final class UInt8 extends MemoryBase {}
final class UInt16 extends MemoryBase {}
final class UInt32 extends MemoryBase {}
final class Int8 extends MemoryBase {}
final class Int16 extends MemoryBase {}
final class Int32 extends MemoryBase {}

final class MemoryPointer extends Memory {
	private $item;
	private $itemRaw;
	public function Set( $value ) {
		$this->GetIO()->WriteUInt32( $this->OffsetOf() , $value );
		return $this;
	}
	public function Get() {
		return $this->GetIO()->ReadUInt32( $this->OffsetOf() );
	}
	public function onConstruct( $params ) {
		$this->SetSize(4);
		$this->itemRaw = $params['item'];
	}
	public function To() {
		if ( !$this->item ) {
			$this->item = $this->ParseTypeRaw( $this->itemRaw );
		}
		$this->item->SetOffset( $this->Get() );
		return $this->item;
	}
	
	public function __get($name) {
		$name = strtolower($name);
		return $this->To();
	}
	public function __set($name, $value) {
		$name = strtolower($name);
		$this->To()->Set($value);
		return $this;
	}
	public function __invoke() {
		return $this->To();
	}
}

final class MemoryArray extends Memory implements \ArrayAccess {
	private $item;
	private $count;

	public function CountOf() {
		return $this->count;
	}
	
	private function __checkIndex(&$index) {
		$index = (int)$index;
		return !( $index < 0 || $index >= $this->count );
	}
	private function __tryIndex(&$index) {
		if ( !$this->__checkIndex($index) ) { $this->riseError("Index {$index} of bounds error"); }
	}


	public function Get() {
		return $this;
	}
	public function Set($value) {
		if ( ( !$value instanceof Memory ) ) {
			$this->riseError( "Error type value" );
		}
		if ( $value->SizeOf() !== $this->SizeOf() ) {
			$this->riseError( "Error size value({$value->SizeOf()}), need({$this->SizeOf()})" );
		}
		$this->GetIO()->Write($this->OffsetOf(), $value->GetIO()->OffsetOf());
	}
	
    public function offsetExists($index) {
		return $this->__checkIndex($index);
    }
    public function offsetGet($index) {
		$this->__tryIndex($index);
		$this->item->SetOffset($this->OffsetOf() + $index * $this->item->SizeOf());
		if ( $this->item instanceof MemoryPointer ) {
			return $this->item;
		}
		return $this->item->Get();
    }
    public function offsetSet($index, $value) {
		$this->__tryIndex($index);
		$this->item->SetOffset($this->OffsetOf() + $index * $this->item->SizeOf());
		$this->item->Set($value);
    }
    public function offsetUnset($offset) {
    }
	
	public function onConstruct($params) {
		$this->item = $this->ParseTypeRaw( $params['item'] );
		$this->count = $params['count'];
		$this->SetSize( $this->item->SizeOf() * $this->count );
	}
}

final class MemoryOffset extends Memory {
	public function Set( $value ) {
	}
	public function Get() {
		return $this->OffsetOf();
	}

	public function onConstruct() {
		$this->SetSize(0);
	}
}

class MemoryStructure extends Memory {
	private $propertiesRaw;
	private $properties;
	private $propertiesOffsets;
	public function onConstruct() {
		$this->__PropetiesParseRaw();
		$this->__PropetiesParse();
	}
	
	private function __PropetiesParseRaw() {
		$this->propertiesRaw = [];
		foreach((array)$this as $property => $type) {
			if ( $property[0] === "\x00" ) { continue; }
			$this->propertiesRaw[ $property ] = $type;
			unset($this->{$property});
		}
	}
	private function __PropetiesParse() {
		$this->properties = [];
		$this->propertiesOffsets = [];
		foreach($this->propertiesRaw as $propertyRaw => $typeRaw) {
			$this->__PropetyParse($propertyRaw, $typeRaw);
		}
	}
	private function __PropetyParse($name, $typeRaw) {
		$name = strtolower(trim($name));
		$typeRaw = strtolower(trim($typeRaw));

		$this->properties[$name] = $type = $this->ParseTypeRaw($typeRaw);
		$this->propertiesOffsets[$name] = $this->SizeOf();
		$this->SetSize( $this->SizeOf() + $type->SizeOf() );
	}

	public function __get($name) {
		$name = strtolower($name);
		if ( isset( $this->properties[$name] ) ) {
			$clone = clone $this->properties[$name];
			$clone->SetOffset( $this->OffsetOf() + $this->propertiesOffsets[$name] );
			return $clone->Get();
		}
	}
	public function __set($name, $value) {
		$name = strtolower($name);
		if ( isset( $this->properties[$name] ) ) {
			$clone = clone $this->properties[$name];
			$clone->SetOffset( $this->OffsetOf() + $this->propertiesOffsets[$name] );
			return $clone->Set($value);
		}
	}
	public function __call($name, $args) {
		$name = strtolower($name);
		if ( isset( $this->properties[$name] ) ) {
			$clone = clone $this->properties[$name];
			$clone->SetOffset( $this->OffsetOf() + $this->propertiesOffsets[$name] );
			if ( $clone instanceof MemoryPointer ) {
				return $clone->To();
			}
			return $clone;
		}
	}
	
	public function Get() {
		return $this;
	}
	public function Set($value) {
		return $this;
	}
}
//**********************************

//**********************************
//**********************************
//**********************************


//**********************************
class _KVLAvlNodeMemoryStructure extends MemoryStructure {
	public $key = "uint32";
	public $left = "*_KVLAvlNodeMemoryStructure";
	public $right = "*_KVLAvlNodeMemoryStructure";
	public $height = "uint32";
	public $next = "*_KVLAvlNodeMemoryStructure";
	public $prevItem = "*_KVLAvlNodeMemoryStructure";
	public $nextItem = "*_KVLAvlNodeMemoryStructure";
	public $lengthKey = "uint32";
	public $lengthValue = "uint32";
	public $dataOffset = "<";
	public function GetKey() {
		return $this->GetIO()->ReadBuffer($this->dataOffset, $this->lengthKey);
	}
	public function GetValue() {
		return $this->GetIO()->ReadBuffer($this->dataOffset + $this->lengthKey, $this->lengthValue);
	}
	public function SetKeyValue($key, $value) {
		$this->lengthKey = strlen($key);
		$this->lengthValue = strlen($value);
		$this->GetIO()->WriteBuffer($this->dataOffset, $key . $value);
	}

	use CommonAvlNode;

	public function Remove($key, $keyData, $tree, $memory) {
		if( $key < $this->key ) {
			if ( !$this->left ) { return $this->OffsetOf(); }
			$this->left = $this->left()->Remove($key, $keyData, $tree, $memory);
		} elseif( $key > $this->key ) {
			if ( !$this->right ) { return $this->OffsetOf(); }
			$this->right = $this->right()->Remove($key, $keyData, $tree, $memory);
		} else {
			if ( $this->GetKey() === $keyData ) {
//echo "	Remove: first\n";
				$this->DelNodeTransfer($tree);
				$tree->DecCount();
				if ( $this->next ) {
//echo "	Remove: first: is next\n";
					$nextNode = $this->next();
					$nextNode->left = $this->left;
					$nextNode->right = $this->right;
					$nextNode->height = $this->height;
					return $this->next;
				} else {
					$q = $this->left;
					$r = $this->right;
					if ( !$r ) {
						$memory->Free($this->OffsetOf());
						return $q;
					}
//echo "	Remove: first: process\n";
					$min = $this->right()->Findmin();
					$min->right = $this->right()->Removemin();
					$min->left = $q;
					$memory->Free($this->OffsetOf());
					return $min->Balance();
				}
			} else {
				$node = $this;
				while($node->next) {
					if ( $node->next()->GetKey() === $keyData ) {
						$node->next()->DelNodeTransfer($tree);
						
						$node->next = $node->next()->next;
						$tree->DecCount();
					}
					$node = $node->next();
				}
				return $this;
			}
		}
		return $this->Balance();
	}
	
	public function Insert(_KVLAvlNodeMemoryStructure $node, _KVLAvlTreeMemoryStructure $tree, $memory) {
		$key = $node->key;
		if ( $key < $this->key ) {
			$this->left = $this->left ? $this->left()->Insert($node, $tree, $memory) : $node->OffsetOf( $tree->IncCount( $node ) );
		} elseif ( $key > $this->key ) {
			$this->right = $this->right ? $this->right()->Insert($node, $tree, $memory) : $node->OffsetOf( $tree->IncCount( $node ) );
		} else {
			$dataKey = $node->GetKey();
			if ( $this->GetKey() === $dataKey ) {
				$node->SetNodeTransfer( $this , $tree );
				$memory->Free($this->OffsetOf());
				return $node->OffsetOf();
			} else {
				$eachNode = $this;
				while( $eachNode->next ) {
					if ( $eachNode->next()->GetKey() === $dataKey ) {
						$node->SetNodeTransfer( $eachNode->next() , $tree );
						
						$node->next = $eachNode->next()->next;
						$memory->Free( $eachNode->next );
						$eachNode->next = $node->OffsetOf();
						break;
					}
				}
				$eachNode->next = $node->OffsetOf();
				
				$tree->IncCount( $node );
			}
			return $this->OffsetOf();
		}
		
		return $this->Balance();
	}
	
	private function SetNodeTransfer( $src, $tree ) {
		$this->left = $src->left;
		$this->right = $src->right;
		$this->height = $src->height;
		$this->next = $src->next;
		$this->prevItem = $src->prevItem;
		$this->nextItem = $src->nextItem;
	
		if ( $this->prevItem ) { 
			$this->prevItem()->nextItem = $this->OffsetOf(); 
		} else {
			$tree->baseNode = $this->OffsetOf();
		}
		if ( $this->nextItem ) {
			$this->nextItem()->prevItem = $this->OffsetOf(); 
		} else {
			$tree->lastNode = $this->OffsetOf();
		}
	}
	private function DelNodeTransfer( $tree ) {
		if ( $this->prevItem ) {
			$this->prevItem()->nextItem = $this->nextItem;
		} else {
			$tree->baseNode = $this->nextItem;
		}
		if ( $this->nextItem ) {
			$this->nextItem()->prevItem = $this->prevItem;
		} else {
			$tree->lastNode = $this->prevItem;
		}
	}
}
class _KVLAvlTreeMemoryStructure extends MemoryStructure {
	public $mark = "[16]uint8";
	public $node = "*_KVLAvlNodeMemoryStructure";
	public $baseNode = "*_KVLAvlNodeMemoryStructure";
	public $lastNode = "*_KVLAvlNodeMemoryStructure";
	public $nextIndex = "uint32";
	public $count = "uint32";

	public function IncCount( $node = null ) {
		$this->count = $this->count + 1;
		if ( $node !== null ) {
			$node->nextItem = 0;
			$node->prevItem = 0;
			if ( $this->lastNode ) {
				$this->lastNode()->nextItem = $node->OffsetOf();
				$node->prevItem = $this->lastNode;
			}
			$this->lastNode = $node->OffsetOf();
		}
	}
	public function DecCount() {
		$this->count = $this->count - 1;
	}
	
	public function SetNode(_KVLAvlNodeMemoryStructure $node, $memory) {
		$node->left = 0;
		$node->right = 0;
		$node->height = 0;
		$node->prev = 0;
		$node->next = 0;
		if ( !$this->node ) {
			$this->node = $node->OffsetOf();
			$this->baseNode = $node->OffsetOf();
			$this->IncCount( $node );
			return;
		}
		
		$this->node = $this->node()->Insert( $node , $this , $memory );
	}
	public function Search($key, $keyData) {
		if ( !$this->node ) { return; }
		$node = $this->node();
		$key = (int)$key;
		while(1) {
			if ( $key < $node->key ) {
				if ( !$node->left ) { return; }
				$node = $node->left(); 
				continue; 
			}
			if ( $key > $node->key ) { 
				if ( !$node->right ) { return; }
				$node = $node->right(); 
				continue; 
			}
			if ( $node->GetKey() === $keyData ) {
				return $node;
			}
			while($node->next) {
				if ( $node->next()->GetKey() === $keyData ) {
					return $node->next();
				}
				$node = $node->next();
			}
			return;
		}
	}
	public function Remove($key, $keyData, $memory) {
		if ( !$this->node ) { return; }
		$key = (int)$key;
		$this->node = $this->node()->Remove($key, $keyData, $this, $memory);
	}	
}

class MemoryKeyValueList implements \ArrayAccess,\Iterator,\Countable {
	private $C_MARK_OFFSET = 20;
	private $C_MARK = [0xEB,0x41,0x3F,0x3B,0x58,0x92,0xB0,0x86,0xA1,0x14,0x5A,0x0E,0x96,0xFE,0x14,0xA8,];
	private $file;
	private $memoryRaw;
	private $reader;
	private $memory;
	private $count = 0;
	private $position;
	private $alv;
	public function __construct( $path ) {
		$this->file = new FileManager( $path );
		$this->file->Open()->Lock();
		$this->memoryRaw = new File_MemoryRawIO( $this->file );
		$this->reader = new MemoryReader( $this->memoryRaw );
		$this->memory = new MemoryManager( $this->memoryRaw, $this->reader );

		$alv = new _KVLAvlTreeMemoryStructure($this->reader, $this->C_MARK_OFFSET);
		if ( $this->memoryRaw->GetSize() < $this->C_MARK_OFFSET + $alv->SizeOf() ) {
			$mem = $this->memory->Allocate( $alv->SizeOf() );
			foreach($this->C_MARK as $i => $v) { $alv->mark[$i] = $v; }
			$alv->node = 0;
		}

		$m1 = $this->memory->Allocate( mt_rand(1,999) );
		$m2 = $this->memory->Allocate( mt_rand(1,999) );
		
		$this->memory->Free( $m2 );
		$this->memory->Free( $m1 );

		foreach($this->C_MARK as $i => $v) {
			if ( $alv->mark[$i] !== $v ) { throw new \Exception("Mark error"); }
		}
		
		$this->alv = $alv;
	}
	
	public function Get( $key ) {
		$this->doKeyValue($key);
		return $this->alv->Search( $this->key , $this->keyData );
	}
	public function Set( $key , $value ) {
		if ( $key === null ) {
			$key = $this->alv->nextIndex;
			$this->alv->nextIndex = $this->alv->nextIndex + 1;
		}
		$this->doKeyValue($key, $value);

		if ( (string)$key === (string)(int)$key ) {
			$this->alv->nextIndex = max($this->alv->nextIndex, $key);
		}
		
		$node = new _KVLAvlNodeMemoryStructure($this->reader, 0);
		$node->SetOffset(
			$_=$this->memory->Allocate( $node->SizeOf() + $this->sizeData )
		);
		
		$node->key = $this->key;
		$node->left = 0;
		$node->right = 0;
		$node->height = 0;
		$node->prev = 0;
		$node->next = 0;
		$node->nextItem = 0;
		$node->SetKeyValue( $this->keyData , $this->valueData );
		
		$this->alv->SetNode( $node, $this->memory );
		
	}
	public function Del( $key ) {
		$this->doKeyValue($key);
		return $this->alv->Remove( $this->key , $this->keyData , $this->memory);
	}

	public function offsetExists( $key ) {
		return $this->Get( $key ) !== null;
	}
	public function offsetGet( $key ) {
		if ( null !== $node = $this->Get( $key ) ) {
			return $this->rtValue( $node->GetValue() );
		}
		return null;
	}
	public function offsetSet( $key, $value ) {
		$this->Set( $key, $value );
	}
	public function offsetUnset( $key ) {
		$this->Del( $key );
	}

	public function current() {
		if ( !$this->position ) { return; }
		return $this->rtValue( $this->position->GetValue() );
	}
	public function key() {
		if ( !$this->position ) { return; }
		return $this->position->GetKey();
		
	}
	public function next() {
		if ( !$this->position ) { return; }
		if ( !$this->position->nextItem ) {
			$this->position = null;
		} else {
			$this->position = $this->position->nextItem();
		}
	}
	public function rewind() {
		if ( !$this->alv->count ) {
			$this->position = null;
		} else {
			$this->position = $this->alv->baseNode();
		}
	}
	public function valid() {
		return (booL)$this->position;
	}
	
	private $key;
	private $keyData;
	private $valueData;
	private $sizeData;
	private function doKeyValue( $key , $value = null ) {
		$md5 = substr(md5($key), 0, 8);
		$this->key = unpack("l", hex2bin( $md5 ) )[1];// & ~(1 << 31);
		$this->keyData = $key;
		$this->valueData = serialize($value);
		@$this->sizeData = strlen($this->keyData) + strlen($this->valueData);
	}
	private function rtValue( $value ) {
		if (!$value) {
			return $value;
		}
		return unserialize($value);
	}
	
	public function Count() { return $this->alv->count; }

	public function pvTest( $addLog , $w = 10 , $range = [1,100] , $rangeModeCreateKey = [0,4] ) {
		set_time_limit(0);
		$dataList = [];
		while($w--) {
			$tmpList = [];
			$aCount = 0;
			for($i=0, $c=mt_rand($range[0], $range[1]); $i<$c; $i++) {
				$key = microtime(1). dechex( mt_rand(0,999999) );
				if ( !mt_rand($rangeModeCreateKey[0], $rangeModeCreateKey[1]) ) {
					$key = "s".mt_rand(0,20);
				}
				$val = microtime(1). dechex( mt_rand(0,999999) ) . uniqid("",1);
				
				$this[ $key ] = $val;
				$dataList[$key] = $val;
				
				$aCount++;
			}
			
			$keyList = array_keys($dataList);
			shuffle($keyList);
			$_ = []; foreach($keyList as $k) { $_[$k] = $dataList[$k]; }
			$dataList = $_;
			
			$fCount = 0;
			for($i=0, $c=mt_rand(1,count($dataList)); $i<$c; $i++) {
				reset($dataList);
				$key = key($dataList);
				$value = current($dataList);
				if ( $this[$key] !== $value ) {
					$addLog( "Error: key: $key, \"$value\" !== \"{$this[$key]}\" \n" );
				}
				unset($dataList[key($dataList)]);
				unset($this[$key]);
				$fCount++;
			}
			
			$addLog( "ACount: $aCount\n" );
			$addLog( "FCount: $fCount\n-------------\n" );
			
			$eCount = 0;
			foreach($this as $key=>$value) {
				if ( !isset($dataList[$key]) ) {
					$addLog( "Error: foreach key: $key\n" );
				}
				$eCount++;
			}
			if ( $eCount !== count($dataList) ) {
				$addLog( "Error: foreach coutn != real count: $key\n" );
			}
			$addLog( "ECount: $eCount\n-------------\n" );
		}

		$fCount = 0;
		while(count($dataList)) {
				reset($dataList);
				$key = key($dataList);
				$value = current($dataList);
				if ( $this[$key] !== $value ) {
					$addLog( "Error: key: $key, \"$value\" !== \"{$this[$key]}\" \n" );
				}
				unset($dataList[key($dataList)]);
				unset($this[$key]);
				$fCount++;
		}
		
		$addLog( "FCount: $fCount\n-------------\n" );
	}
}


