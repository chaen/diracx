# DiracX data management

## Abstract

DiracX will have native Data Management (DMS) capabilities. They will be based on primitives proven by DIRAC, with revisited defaults which makes more sense for todays technology and to make it trivial to use diracx DMS. Advanced features which currently live in LHCbDIRAC and which are useful for a wider community (like popularity) will be integrated.

The basic primitives of Data Management are:

- Logical File Name (LFN): path like string, max 512 char. Unique identifier of a file throughout the namespace of a VO. It has to start with "/<vo>". A "Replica" is a physical instance of the LFN
- FileCatalog (FC): abstract interface to a replica catalog, storing for each LFN properties (owner, size, ADLER32 checksum, etc) and replica location
- Storage Element (SE): abstrace interface to a physical storage. An LFN replica is stored at a StorageElement

## FileCatalog

The concept of FileCatalog is inherited from DIRAC. It is an interface to the Replica catalog.

DIRAC could handle multiple catalog backends in parallel, one of them being the Master. This turned out not to be used by anybody except LHCb.

diracx will give up that option. It will still define an abstract interface, but not offer the looping features anymore. This will avoid concistency problem and error reporting issues.
The actual implementation of the catalog used is a diracx entrypoint. By default, it should be the DFC.

The usecase of LHCb is to toggle a flag in a secondary catalog, only for production data, whenever we create or delete an LFN. This can be covered by using a dedicated FileCatalog implementation, just overriding these 2 methods.

Open question: in a multi vo installation, are all the VO using the same FileCatalog or not necessarily ?

In DIRAC, the `FileCatalog` object would always go through DISET services. In `diracx`, it is desirable to have a similar abstraction for client and for server side interactions (typically when tasks need to run)

DIRAC used to store and retrieve PFN/URL, however there is no need to keep that feature anymore: urls are constructed on the fly (LFN/PFN convention: SE BasePath + LFN)

## Storage Elements

The Storage Element (SE) is an abstraction of a physical storage endpoint, inherited from DIRAC, and carying over most of its features.
It hides away the low level details of the endpoint (protocol, hostname, basepath) to any of the client.

A given storage endpoint can have multiple SE pointing at it.
A given SE can have multiple protocols, with ordered preference list, just like DIRAC. The SE makes the distinction between local, remote only, or remote protocols.

Each protocol is managed by a plugin. The plan is to support xroot, https and s3 if communities are interested.

A StorageElementBase is an abstraction of a StorageElement, such that we can factorize configuration

A StorageElementGroup is a list of StorageElements. They are used to group SEs in a way that makes sense for Data distribution (for example Tier1-MC).

The SE objects allows direct interactions with the physical storage (get file metadata, download file, put file).

DIRAC supported handling third party copy (TPC), mostly via the gfal2 library. Although it is accepted by all the communities that TPC should go via FTS, diracx could still support local TPC (via local streaming if needed) as all the logic is already existing in DIRAC and is trivial to port. Tape staging however will not be supported.

OpenQuestion: do we want an ADR for the Resource configuration section ?
OpenQuestion: how do tasks interact with storages ? Tokens only ?

## Metadata

The definition of a MetaData is very experiment specific.

## DFC

DIRAC has its own implementation of a replica catalog: the DFC. It had multiple managers, such that VOs could choose. In practie, only 2 sets are used:

- LHCb relies on the FileCatalogWithPSAndFk schema and the corresponding sets

    ```
      FileManager = FileManagerPs
      DirectoryManager = DirectoryClosure
      SecurityManager = VOMSSecurityManager
      SecurityPolicy = DIRAC/DataManagementSystem/DB/FileCatalogComponents/SecurityPolicies/VOMSPolicy
    ```

- Others rely on the defaults managers.

LHCb managers has proven to be the most performant so the schema will be based on this one. There will however not be any stored procedure.

OpenQuestion: do we keep status, for example for a "ToBeDeleted" status ? It is a bit completed to manage and keep in sync if the metadata catalog is not kept in sync

OpenQuestion: ownership should be a separate table or not ?
OpenQuestion: checksum. Do we accept that we stick to Adler32 and that if ever anything changes, we need to revisit it ?
OpenQuestion: how to store the Adler32 ? INT UNSIGNED makes sense but what do we get from the tools ?

### Schema

```sql
CREATE TABLE FC_Statuses (
    StatusID INT AUTO_INCREMENT,
    Status VARCHAR(32),

    PRIMARY KEY (StatusID),

    UNIQUE(Status)
) ENGINE = INNODB;

INSERT INTO FC_Statuses (StatusID, Status) values (1, 'FakeStatus');


CREATE TABLE FC_StorageElements (
    SEID INTEGER AUTO_INCREMENT,
    SEName VARCHAR(127) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,

    PRIMARY KEY (SEID),

    UNIQUE (SEName)
)

INSERT INTO FC_StorageElements (SEID, SEName) values (1, 'FakeSE');


-- Do we need it or do we just store vo/sub for each Directory/File ?
CREATE TABLE FC_Ownership (
    int ownerID
    varchar VO
    varchar Sub
) ENGINE = INNODB;



create table FC_DirectoryList (
  DirID BIGINT NOT NULL AUTO_INCREMENT,
  OwnerID INT,
  Sub varchar
  CreationDate DATETIME,
  -- do we need it ?
  ModificationDate DATETIME,
  Name varchar(255)CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
)



CREATE TABLE FC_DirectoryClosure (
 ParentID BIGINT NOT NULL,
 ChildID BIGINT NOT NULL,
 Depth INT NOT NULL DEFAULT 0,

)


CREATE TABLE FC_Files(
    FileID BIGINT AUTO_INCREMENT,
    DirID BIGINT NOT NULL,
    Size BIGINT UNSIGNED NOT NULL,
    OwnerID INT NOT NULL,
    -- Maybe....
    Status INT NOT NULL,
    -- Gaudi GUID
    GUID char(36) NOT NULL,
    lfn_sha256  BINARY(32),
    CreationDate DATETIME,
    Adler32 INT UNSIGNED,
    FileName VARCHAR(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,

) ENGINE = INNODB;


CREATE TABLE FC_Replicas (
    ReplicaID BIGINT AUTO_INCREMENT,
    FileID BIGINT NOT NULL,
    SEID INTEGER NOT NULL,
    -- keep it ?
    Status INTEGER NOT NULL,
    CreationDate DATETIME,
)
```

Counters following ADR-009 needs to be added for the number of files and the size per directory, non recursive. It replaces the FC_DirectoryUsage table.

Keeping a directory structure and hierarchy is a strong wish from the community, and it will stay so. This is primarily because directory are used as a dataset definition.
Although the directory hierarchy is implement using the Closure table, most operations following insertions are done on files. This is why a sha256, based on the LFN is added. It allows operations directly on files, without traversing directories.

DIRAC offers support for metadata and dataset. diracx will offer something similar:

- MetaData can be associated to files or directories. They are recursive, and cumulative. They are stored in separate tables as JSON columns. Metadata should be searchable, that is from a list of metadata, we get a list of LFNs. But from a given LFN we need to be able to extract all its metadata.

- Datasets are not really used.

- OpenQuestion: do we want to allow for "annotation", that is meta data associated to files/dir which are specific to a user (e.g. "my sets of LFNs"), as opposed to metadata which are the same for everybody (e.g. Collision25)

- openQuestion: do we want to have frozen dataset ?

- openQuestion: do we want to have ancestry ?

### DFC security policy

The current DFC security policy relies on user identity (owner name and group) and matches it against the modes stored in the DB.

diracx should make use of its security policy and tokens. The diracx token should contain a dedicated section listing the specific permissions a token has on the namespace. Validation can then be performed by simple pattern matching.

The possible primitives are:

- read: download file, list directory, etc
- write: create file, create subdirectory
- delete: remove the file or subdirectories
- replicate: allow to replicate a file to another SE (could be replaced with read+write)
- full: all of the above

```json
"dfc" :
{
    "/": "read",
    "/lhcb/user/c/chaen": "full",
}
```

Given that this is managed by the security policy, any extension could chose how they want to issue these rules and how they want to verify them.

The SE tokens could then be issued based on these permissions

## FTS

FTS will be the primary way to transfer data and manage staging. Transfers will still originate from the RMS (see TS ADRs)

We want to support Activity and shares.
Possibly use only FTS4 client.
We need a new component to listen to the Message queue and update the status.
Current FTS3Agent needs to be replaced with tasks

## Integration

- default post steps in the JobWrapper: register file metadata (trans ID, workgraph ID, etc). Send usage information

- Quota managements between SEs, for example:

    - always keep 1 PB free for buffer
    - no separation between dST/MC DST
    - plan to have archive limited because we plan X for sprucing

## Rucio

FC object: no direct DB access for now, but propose a "system account token".

## Future work

- Popularity
- Consistency check
- usage report
- TS: pre-staging
- TS: feeder on DFC metadata, directory listing
