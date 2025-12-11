<?php

namespace Utopia\Database\Adapter;

use Utopia\Database\Adapter;
use Utopia\Database\Database;
use Utopia\Database\Document;
use Utopia\Database\Exception as DatabaseException;
use Utopia\Database\Query;

/**
 * Clickhouse adapter backed by the HTTP interface.
 *
 * This adapter intentionally implements a minimal subset of the interface.
 * Most advanced features (attributes, relationships, operators, etc.) are
 * not available in ClickHouse and will throw a DatabaseException.
 */
class Clickhouse extends Adapter
{
    private string $endpoint;
    private string $username;
    private string $password;

    /**
     * @param string $endpoint ClickHouse HTTP endpoint (e.g. http://127.0.0.1:8123)
     * @param string $username
     * @param string $password
     * @param string $database
     */
    public function __construct(
        string $endpoint = 'http://127.0.0.1:8123',
        string $username = 'default',
        string $password = '',
        string $database = ''
    ) {
        $this->endpoint = rtrim($endpoint, '/');
        $this->username = $username;
        $this->password = $password;

        if (!empty($database)) {
            $this->setDatabase($database);
        }
    }

    public function setTimeout(int $milliseconds, string $event = Database::EVENT_ALL): void
    {
        if ($milliseconds < 0) {
            throw new DatabaseException('Timeout must be greater than or equal to 0');
        }

        $this->timeout = $milliseconds;
    }

    public function startTransaction(): bool
    {
        // ClickHouse does not support transactions in the same sense.
        return false;
    }

    public function commitTransaction(): bool
    {
        return false;
    }

    public function rollbackTransaction(): bool
    {
        return false;
    }

    protected function quote(string $string): string
    {
        return '`' . $this->filter($string) . '`';
    }

    public function ping(): bool
    {
        $this->run('SELECT 1 FORMAT JSON', useDatabase: false);

        return true;
    }

    public function reconnect(): void
    {
        // Stateless HTTP - nothing to do.
    }

    public function create(string $name): bool
    {
        $name = $this->filter($name);
        $this->run("CREATE DATABASE IF NOT EXISTS {$name}", useDatabase: false);

        return true;
    }

    public function exists(string $database, ?string $collection = null): bool
    {
        $database = $this->filter($database);

        if (empty($collection)) {
            $result = $this->run("
                SELECT name FROM system.databases
                WHERE name = '{$database}'
                LIMIT 1
                FORMAT JSON
            ", useDatabase: false);

            return !empty($result['data']);
        }

        $collection = $this->filter($collection);
        $table = $this->quote($this->getNamespace() . '_' . $collection);

        $result = $this->run("
            SELECT name FROM system.tables
            WHERE database = '{$database}' AND name = {$table}
            LIMIT 1
            FORMAT JSON
        ", useDatabase: false);

        return !empty($result['data']);
    }

    public function list(): array
    {
        $result = $this->run('SHOW DATABASES FORMAT JSON', useDatabase: false);

        $list = [];
        foreach ($result['data'] ?? [] as $row) {
            $list[] = new Document(['$id' => $row['name'] ?? '']);
        }

        return $list;
    }

    public function delete(string $name): bool
    {
        $name = $this->filter($name);
        $this->run("DROP DATABASE IF EXISTS {$name}", useDatabase: false);

        return true;
    }

    public function createCollection(string $name, array $attributes = [], array $indexes = []): bool
    {
        $table = $this->quote($this->getSQLTable($this->filter($name)));

        $tenantColumn = $this->sharedTables ? ", _tenant Nullable(Int32)" : '';

        $sql = "
            CREATE TABLE IF NOT EXISTS {$table} (
                _id UUID DEFAULT generateUUIDv4(),
                _uid String,
                _createdAt DateTime64(3),
                _updatedAt DateTime64(3),
                _permissions String,
                _data String
                {$tenantColumn}
            )
            ENGINE = MergeTree()
            ORDER BY (_id)
        ";

        $this->run($sql);

        return true;
    }

    public function deleteCollection(string $id): bool
    {
        $table = $this->quote($this->getSQLTable($this->filter($id)));
        $this->run("DROP TABLE IF EXISTS {$table}");

        return true;
    }

    public function analyzeCollection(string $collection): bool
    {
        // OPTIMIZE TABLE refreshes statistics.
        $table = $this->quote($this->getSQLTable($this->filter($collection)));
        $this->run("OPTIMIZE TABLE {$table}");

        return true;
    }

    public function createAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false, bool $required = false): bool
    {
        throw new DatabaseException('Attributes are not supported by the Clickhouse adapter');
    }

    public function createAttributes(string $collection, array $attributes): bool
    {
        throw new DatabaseException('Attributes are not supported by the Clickhouse adapter');
    }

    public function updateAttribute(string $collection, string $id, string $type, int $size, bool $signed = true, bool $array = false, ?string $newKey = null, bool $required = false): bool
    {
        throw new DatabaseException('Attributes are not supported by the Clickhouse adapter');
    }

    public function deleteAttribute(string $collection, string $id): bool
    {
        throw new DatabaseException('Attributes are not supported by the Clickhouse adapter');
    }

    public function renameAttribute(string $collection, string $old, string $new): bool
    {
        throw new DatabaseException('Attributes are not supported by the Clickhouse adapter');
    }

    public function createRelationship(string $collection, string $relatedCollection, string $type, bool $twoWay = false, string $id = '', string $twoWayKey = ''): bool
    {
        throw new DatabaseException('Relationships are not supported by the Clickhouse adapter');
    }

    public function updateRelationship(string $collection, string $relatedCollection, string $type, bool $twoWay, string $key, string $twoWayKey, string $side, ?string $newKey = null, ?string $newTwoWayKey = null): bool
    {
        throw new DatabaseException('Relationships are not supported by the Clickhouse adapter');
    }

    public function deleteRelationship(string $collection, string $relatedCollection, string $type, bool $twoWay, string $key, string $twoWayKey, string $side): bool
    {
        throw new DatabaseException('Relationships are not supported by the Clickhouse adapter');
    }

    public function renameIndex(string $collection, string $old, string $new): bool
    {
        throw new DatabaseException('Indexes are not supported by the Clickhouse adapter');
    }

    public function createIndex(string $collection, string $id, string $type, array $attributes, array $lengths, array $orders, array $indexAttributeTypes = []): bool
    {
        throw new DatabaseException('Indexes are not supported by the Clickhouse adapter');
    }

    public function deleteIndex(string $collection, string $id): bool
    {
        throw new DatabaseException('Indexes are not supported by the Clickhouse adapter');
    }

    public function getDocument(Document $collection, string $id, array $queries = [], bool $forUpdate = false): Document
    {
        $table = $this->quote($this->getSQLTable($this->filter($collection->getId())));
        $conditions = ["_uid = '" . $this->escape($id) . "'"];

        if ($this->sharedTables) {
            $conditions[] = $this->getTenantCondition();
        }

        $where = implode(' AND ', $conditions);

        $result = $this->run("
            SELECT _id, _uid, _createdAt, _updatedAt, _permissions, _data" . ($this->sharedTables ? ", _tenant" : '') . "
            FROM {$table}
            WHERE {$where}
            LIMIT 1
            FORMAT JSON
        ");

        if (empty($result['data'])) {
            return new Document([]);
        }

        return $this->hydrateDocument($result['data'][0]);
    }

    public function createDocument(Document $collection, Document $document): Document
    {
        $table = $this->quote($this->getSQLTable($this->filter($collection->getId())));

        $record = [
            '_uid' => $document->getId(),
            '_createdAt' => $document->getCreatedAt(),
            '_updatedAt' => $document->getUpdatedAt(),
            '_permissions' => json_encode($document->getPermissions()),
            '_data' => json_encode($document->getAttributes()),
        ];

        if (!empty($document->getSequence())) {
            $record['_id'] = $document->getSequence();
        }

        if ($this->sharedTables) {
            $record['_tenant'] = $document->getTenant();
        }

        $jsonRow = json_encode($record);

        $this->run("
            INSERT INTO {$table} (" . implode(', ', array_keys($record)) . ")
            FORMAT JSONEachRow
            {$jsonRow}
        ", false);

        $fresh = $this->getDocument($collection, $document->getId());
        if (!$fresh->isEmpty()) {
            return $fresh;
        }

        return $document;
    }

    public function createDocuments(Document $collection, array $documents): array
    {
        $created = [];
        foreach ($documents as $document) {
            $created[] = $this->createDocument($collection, $document);
        }

        return $created;
    }

    public function updateDocument(Document $collection, string $id, Document $document, bool $skipPermissions): Document
    {
        throw new DatabaseException('Updates are not supported by the Clickhouse adapter');
    }

    public function updateDocuments(Document $collection, Document $updates, array $documents): int
    {
        throw new DatabaseException('Updates are not supported by the Clickhouse adapter');
    }

    public function upsertDocuments(Document $collection, string $attribute, array $changes): array
    {
        throw new DatabaseException('Upserts are not supported by the Clickhouse adapter');
    }

    public function getSequences(string $collection, array $documents): array
    {
        $missing = [];
        foreach ($documents as $document) {
            if (empty($document->getSequence())) {
                $missing[] = $document->getId();
            }
        }

        if (empty($missing)) {
            return $documents;
        }

        $table = $this->quote($this->getSQLTable($this->filter($collection)));
        $uids = array_map(fn ($id) => "'" . $this->escape($id) . "'", $missing);

        $tenant = '';
        if ($this->sharedTables) {
            $tenant = ' AND ' . $this->getTenantCondition();
        }

        $result = $this->run("
            SELECT _uid, _id
            FROM {$table}
            WHERE _uid IN (" . implode(',', $uids) . ") {$tenant}
            FORMAT JSON
        ");

        $map = [];
        foreach ($result['data'] ?? [] as $row) {
            $map[$row['_uid']] = $row['_id'];
        }

        foreach ($documents as $document) {
            if (isset($map[$document->getId()])) {
                $document['$sequence'] = $map[$document->getId()];
            }
        }

        return $documents;
    }

    public function deleteDocument(string $collection, string $id): bool
    {
        $table = $this->quote($this->getSQLTable($this->filter($collection)));
        $condition = "_uid = '" . $this->escape($id) . "'";

        if ($this->sharedTables) {
            $condition .= ' AND ' . $this->getTenantCondition();
        }

        $this->run("ALTER TABLE {$table} DELETE WHERE {$condition}");

        return true;
    }

    public function deleteDocuments(string $collection, array $sequences, array $permissionIds): int
    {
        $table = $this->quote($this->getSQLTable($this->filter($collection)));
        $ids = array_map(fn ($seq) => "'" . $this->escape($seq) . "'", $sequences);
        if (empty($ids)) {
            return 0;
        }

        $condition = "_id IN (" . implode(',', $ids) . ")";
        if ($this->sharedTables) {
            $condition .= ' AND ' . $this->getTenantCondition();
        }

        $this->run("ALTER TABLE {$table} DELETE WHERE {$condition}");

        return count($sequences);
    }

    public function find(Document $collection, array $queries = [], ?int $limit = 25, ?int $offset = null, array $orderAttributes = [], array $orderTypes = [], array $cursor = [], string $cursorDirection = Database::CURSOR_AFTER, string $forPermission = Database::PERMISSION_READ): array
    {
        $table = $this->quote($this->getSQLTable($this->filter($collection->getId())));
        $conditions = [];

        foreach ($queries as $query) {
            if ($query->getMethod() !== Query::TYPE_EQUAL) {
                continue;
            }

            $attribute = $query->getAttribute();
            $values = $query->getValues();
            if ($attribute === '$id') {
                $in = array_map(fn ($val) => "'" . $this->escape($val) . "'", $values);
                $conditions[] = "_uid IN (" . implode(',', $in) . ")";
            } elseif ($attribute === '$sequence') {
                $in = array_map(fn ($val) => "'" . $this->escape($val) . "'", $values);
                $conditions[] = "_id IN (" . implode(',', $in) . ")";
            }
        }

        if ($this->sharedTables) {
            $conditions[] = $this->getTenantCondition();
        }

        $where = empty($conditions) ? '' : 'WHERE ' . implode(' AND ', $conditions);

        $orderBy = '';
        if (!empty($orderAttributes)) {
            $orders = [];
            foreach ($orderAttributes as $index => $attr) {
                $column = match ($attr) {
                    '$createdAt' => '_createdAt',
                    '$updatedAt' => '_updatedAt',
                    '$sequence' => '_id',
                    '$id' => '_uid',
                    default => null,
                };
                if ($column === null) {
                    continue;
                }
                $direction = $orderTypes[$index] ?? Database::ORDER_ASC;
                $direction = $direction === Database::ORDER_DESC ? 'DESC' : 'ASC';
                $orders[] = "{$column} {$direction}";
            }
            if (!empty($orders)) {
                $orderBy = 'ORDER BY ' . implode(', ', $orders);
            }
        }

        $limitClause = '';
        if (!is_null($limit)) {
            $limitClause = 'LIMIT ' . (int)$limit;
            if (!is_null($offset)) {
                $limitClause .= ' OFFSET ' . (int)$offset;
            }
        }

        $result = $this->run("
            SELECT _id, _uid, _createdAt, _updatedAt, _permissions, _data" . ($this->sharedTables ? ", _tenant" : '') . "
            FROM {$table}
            {$where}
            {$orderBy}
            {$limitClause}
            FORMAT JSON
        ");

        $documents = [];
        foreach ($result['data'] ?? [] as $row) {
            $documents[] = $this->hydrateDocument($row);
        }

        return $documents;
    }

    public function sum(Document $collection, string $attribute, array $queries = [], ?int $max = null): float|int
    {
        throw new DatabaseException('Aggregate operations are not supported by the Clickhouse adapter');
    }

    public function count(Document $collection, array $queries = [], ?int $max = null): int
    {
        $table = $this->quote($this->getSQLTable($this->filter($collection->getId())));

        $conditions = [];
        if ($this->sharedTables) {
            $conditions[] = $this->getTenantCondition();
        }

        $where = empty($conditions) ? '' : 'WHERE ' . implode(' AND ', $conditions);
        $limitClause = '';
        if (!is_null($max) && $max > 0) {
            $limitClause = 'LIMIT ' . (int)$max;
        }

        $result = $this->run("
            SELECT count() AS total
            FROM {$table}
            {$where}
            {$limitClause}
            FORMAT JSON
        ");

        return (int)($result['data'][0]['total'] ?? 0);
    }

    public function getSizeOfCollection(string $collection): int
    {
        $table = $this->filter($collection);
        $database = $this->getDatabase();
        $result = $this->run("
            SELECT sum(bytes) AS total
            FROM system.parts
            WHERE active = 1
              AND database = '{$database}'
              AND table = '{$this->getNamespace()}_{$table}'
            FORMAT JSON
        ", useDatabase: false);

        return (int)($result['data'][0]['total'] ?? 0);
    }

    public function getSizeOfCollectionOnDisk(string $collection): int
    {
        // Use same metric as getSizeOfCollection for ClickHouse
        return $this->getSizeOfCollection($collection);
    }

    public function getLimitForString(): int
    {
        return 0;
    }

    public function getLimitForInt(): int
    {
        return 0;
    }

    public function getLimitForAttributes(): int
    {
        return 0;
    }

    public function getLimitForIndexes(): int
    {
        return 0;
    }

    public function getSupportForSchemas(): bool
    {
        return false;
    }

    public function getSupportForAttributes(): bool
    {
        return false;
    }

    public function getSupportForSchemaAttributes(): bool
    {
        return false;
    }

    public function getSupportForIndex(): bool
    {
        return false;
    }

    public function getSupportForIndexArray(): bool
    {
        return false;
    }

    public function getSupportForCastIndexArray(): bool
    {
        return false;
    }

    public function getSupportForUniqueIndex(): bool
    {
        return false;
    }

    public function getSupportForFulltextIndex(): bool
    {
        return false;
    }

    public function getSupportForFulltextWildcardIndex(): bool
    {
        return false;
    }

    public function getSupportForCasting(): bool
    {
        return false;
    }

    public function getSupportForQueryContains(): bool
    {
        return false;
    }

    public function getSupportForTimeouts(): bool
    {
        return true;
    }

    public function getSupportForRelationships(): bool
    {
        return false;
    }

    public function getSupportForUpdateLock(): bool
    {
        return false;
    }

    public function getSupportForBatchOperations(): bool
    {
        return false;
    }

    public function getSupportForAttributeResizing(): bool
    {
        return false;
    }

    public function getSupportForGetConnectionId(): bool
    {
        return false;
    }

    public function getSupportForUpserts(): bool
    {
        return false;
    }

    public function getSupportForVectors(): bool
    {
        return false;
    }

    public function getSupportForCacheSkipOnFailure(): bool
    {
        return false;
    }

    public function getSupportForReconnection(): bool
    {
        return true;
    }

    public function getSupportForHostname(): bool
    {
        return true;
    }

    public function getSupportForBatchCreateAttributes(): bool
    {
        return false;
    }

    public function getSupportForSpatialAttributes(): bool
    {
        return false;
    }

    public function getSupportForObject(): bool
    {
        return false;
    }

    public function getSupportForSpatialIndexNull(): bool
    {
        return false;
    }

    public function getSupportForOperators(): bool
    {
        return false;
    }

    public function getSupportForIdenticalIndexes(): bool
    {
        return false;
    }

    public function getSupportForOrderRandom(): bool
    {
        return false;
    }

    protected function getAttributeProjection(array $selections, string $prefix = ''): mixed
    {
        // Projection not supported; return null to fetch all stored data.
        return null;
    }

    protected function execute(mixed $stmt): bool
    {
        // HTTP adapter does not use PDO statements.
        return true;
    }

    public function decodePoint(string $wkb): array
    {
        return [];
    }

    public function decodeLinestring(string $wkb): array
    {
        return [];
    }

    public function decodePolygon(string $wkb): array
    {
        return [];
    }

    public function castingBefore(Document $collection, Document $document): Document
    {
        return $document;
    }

    public function castingAfter(Document $collection, Document $document): Document
    {
        return $document;
    }

    public function getSupportForInternalCasting(): bool
    {
        return false;
    }

    public function getSupportForUTCCasting(): bool
    {
        return false;
    }

    public function setUTCDatetime(string $value): mixed
    {
        return $value;
    }

    public function setSupportForAttributes(bool $support): bool
    {
        return false;
    }

    public function getSupportForIntegerBooleans(): bool
    {
        return false;
    }

    public function getCountOfAttributes(Document $collection): int
    {
        return static::getCountOfDefaultAttributes();
    }

    public function getCountOfIndexes(Document $collection): int
    {
        return static::getCountOfDefaultIndexes();
    }

    public function getCountOfDefaultAttributes(): int
    {
        return count(Database::INTERNAL_ATTRIBUTES);
    }

    public function getCountOfDefaultIndexes(): int
    {
        return count(Database::INTERNAL_INDEXES);
    }

    public function getDocumentSizeLimit(): int
    {
        return 0;
    }

    public function getAttributeWidth(Document $collection): int
    {
        return 0;
    }

    public function getKeywords(): array
    {
        return [];
    }

    public function increaseDocumentAttribute(
        string $collection,
        string $id,
        string $attribute,
        int|float $value,
        string $updatedAt,
        int|float|null $min = null,
        int|float|null $max = null
    ): bool {
        throw new DatabaseException('Increment operations are not supported by the Clickhouse adapter');
    }

    public function getConnectionId(): string
    {
        return '';
    }

    public function getInternalIndexesKeys(): array
    {
        return [];
    }

    public function getSchemaAttributes(string $collection): array
    {
        return [];
    }

    public function getTenantQuery(string $collection, string $alias = ''): string
    {
        return '';
    }

    public function getIdAttributeType(): string
    {
        return Database::VAR_UUID7;
    }

    public function getMaxIndexLength(): int
    {
        return 0;
    }

    public function getMaxUIDLength(): int
    {
        return 255;
    }

    public function getMinDateTime(): \DateTime
    {
        return new \DateTime('1970-01-01 00:00:00');
    }

    public function getSupportForBoundaryInclusiveContains(): bool
    {
        return false;
    }

    public function getSupportForSpatialIndexOrder(): bool
    {
        return false;
    }

    public function getSupportForDistanceBetweenMultiDimensionGeometryInMeters(): bool
    {
        return false;
    }

    public function getSupportForSpatialAxisOrder(): bool
    {
        return false;
    }

    public function getSupportForOptionalSpatialAttributeWithExistingRows(): bool
    {
        return false;
    }

    public function getSupportForMultipleFulltextIndexes(): bool
    {
        return false;
    }

    public function getSupportForAlterLocks(): bool
    {
        return false;
    }

    /**
     * Execute an HTTP query against ClickHouse.
     *
     * @param string $sql
     * @param bool $expectJson
     * @return array<string, mixed>|string
     * @throws DatabaseException
     */
    private function run(string $sql, bool $expectJson = true, bool $useDatabase = true): array|string
    {
        $params = [];
        if ($useDatabase && !empty($this->database)) {
            $params['database'] = $this->database;
        }
        if ($this->timeout > 0) {
            $params['max_execution_time'] = $this->timeout / 1000;
        }

        $url = $this->endpoint;
        if (!empty($params)) {
            $url .= '?' . http_build_query($params);
        }

        $curl = curl_init($url);
        curl_setopt($curl, CURLOPT_POST, true);
        curl_setopt($curl, CURLOPT_RETURNTRANSFER, true);
        curl_setopt($curl, CURLOPT_POSTFIELDS, $sql);
        curl_setopt($curl, CURLOPT_HTTPHEADER, ['Content-Type: text/plain', 'Accept: application/json']);

        if (!empty($this->username)) {
            curl_setopt($curl, CURLOPT_USERPWD, "{$this->username}:{$this->password}");
        }

        $response = curl_exec($curl);
        if ($response === false) {
            $error = curl_error($curl);
            curl_close($curl);
            throw new DatabaseException('Clickhouse request failed: ' . $error);
        }

        if (!\is_string($response)) {
            curl_close($curl);
            throw new DatabaseException('Clickhouse request returned non-string response');
        }

        $status = (int)curl_getinfo($curl, CURLINFO_RESPONSE_CODE);
        curl_close($curl);

        if ($status >= 400) {
            throw new DatabaseException('Clickhouse request failed with status ' . $status . ': ' . $response);
        }

        if (!$expectJson) {
            return (string)$response;
        }

        $trimmed = trim($response);
        if ($trimmed === '') {
            return [];
        }

        $decoded = json_decode($trimmed, true);
        if (json_last_error() !== JSON_ERROR_NONE) {
            throw new DatabaseException('Failed to decode Clickhouse response: ' . json_last_error_msg());
        }

        return $decoded;
    }

    /**
     * Build fully qualified table name.
     */
    private function getSQLTable(string $name): string
    {
        return $this->getNamespace() . '_' . $name;
    }

    /**
     * Convert raw row into a Document instance.
     *
     * @param array<string, mixed> $row
     */
    private function hydrateDocument(array $row): Document
    {
        $data = json_decode($row['_data'] ?? '{}', true);
        if (!is_array($data)) {
            $data = [];
        }

        $data['$id'] = $row['_uid'] ?? null;
        $data['$sequence'] = $row['_id'] ?? null;
        $data['$createdAt'] = $row['_createdAt'] ?? null;
        $data['$updatedAt'] = $row['_updatedAt'] ?? null;
        $permissions = $row['_permissions'] ?? '[]';
        $decodedPermissions = json_decode($permissions, true);
        if (is_array($decodedPermissions)) {
            $data['$permissions'] = $decodedPermissions;
        }

        if ($this->sharedTables && array_key_exists('_tenant', $row)) {
            $data['$tenant'] = $row['_tenant'];
        }

        return new Document($data);
    }

    /**
     * Escape scalar values for inline SQL.
     */
    private function escape(string $value): string
    {
        return str_replace("'", "\\'", $value);
    }

    private function getTenantCondition(): string
    {
        return '_tenant = ' . (is_null($this->tenant) ? 'NULL' : (int)$this->tenant);
    }
}
