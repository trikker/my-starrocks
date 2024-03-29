// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.connector.iceberg;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.IcebergTable;
import com.starrocks.catalog.Type;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.connector.PartitionUtil;
import com.starrocks.connector.exception.StarRocksConnectorException;
import org.apache.iceberg.AddedRowsScanTask;
import org.apache.iceberg.ChangelogOperation;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.DeletedDataFileScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.IncrementalChangelogScan;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Set;
import java.util.stream.Collectors;

public class IcebergPartitionUtils {
    private static final Logger LOG = LogManager.getLogger(IcebergPartitionUtils.class);
    public static class IcebergPartition {
        private PartitionSpec spec;
        private StructLike data;
        private ChangelogOperation operation;

        IcebergPartition(PartitionSpec spec, StructLike data, ChangelogOperation operation) {
            this.spec = spec;
            this.data = data;
            this.operation = operation;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof IcebergPartition)) {
                return false;
            }
            IcebergPartition that = (IcebergPartition) o;
            return Objects.equal(spec, that.spec) &&
                    Objects.equal(data, that.data) && operation == that.operation;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(spec, data, operation);
        }
    }

    public static Set<String> getChangedPartitionNames(Table table, long fromTimestampMillis,
                                                       Snapshot toSnapshot) {
        Set<IcebergPartition> changedPartition = getChangedPartition(table, fromTimestampMillis,
                toSnapshot);
        return changedPartition.stream().map(partition -> PartitionUtil.
                convertIcebergPartitionToPartitionName(table.spec(), partition.data)).collect(Collectors.toSet());
    }

    public static Set<IcebergPartition> getChangedPartition(Table table, long fromExclusiveTimestampMillis,
                                                            Snapshot toSnapshot) {
        ImmutableSet.Builder<IcebergPartition> builder = ImmutableSet.builder();
        Snapshot snapShot = toSnapshot;
        if (toSnapshot.timestampMillis() >= fromExclusiveTimestampMillis) {
            // find the first snapshot which it's timestampMillis is less than or equals fromExclusiveTimestampMillis
            while (snapShot.parentId() != null) {
                snapShot = table.snapshot(snapShot.parentId());
                // snapshot is null when it's expired
                if (snapShot == null || snapShot.timestampMillis() <= fromExclusiveTimestampMillis) {
                    break;
                }
            }
            // get incremental changelog scan when find the first snapshot
            // which it's timestampMillis is less than fromExclusiveTimestampMillis
            if (snapShot != null && snapShot.timestampMillis() <= fromExclusiveTimestampMillis) {
                IncrementalChangelogScan incrementalChangelogScan = table.newIncrementalChangelogScan().
                        fromSnapshotExclusive(snapShot.snapshotId()).toSnapshot(toSnapshot.snapshotId());
                try (CloseableIterable<ChangelogScanTask> tasks = incrementalChangelogScan.planFiles()) {
                    for (ChangelogScanTask task : tasks) {
                        ChangelogOperation operation = task.operation();
                        if (operation == ChangelogOperation.INSERT) {
                            AddedRowsScanTask addedRowsScanTask = (AddedRowsScanTask) task;
                            StructLike data = addedRowsScanTask.file().partition();
                            builder.add(new IcebergPartition(addedRowsScanTask.spec(), data, operation));
                        } else if (operation == ChangelogOperation.DELETE) {
                            DeletedDataFileScanTask deletedDataFileScanTask = (DeletedDataFileScanTask) task;
                            StructLike data = deletedDataFileScanTask.file().partition();
                            builder.add(new IcebergPartition(deletedDataFileScanTask.spec(), data, operation));
                        } else {
                            LOG.warn("Do not support this iceberg change log type, operation is {}", operation);
                        }
                    }
                } catch (Exception e) {
                    LOG.warn("get incrementalChangelogScan failed", e);
                    return getAllPartition(table);
                }
                return builder.build();
            }
        }

        return getAllPartition(table);
    }

    public static Set<IcebergPartition> getAllPartition(Table table) {
        ImmutableSet.Builder<IcebergPartition> builder = ImmutableSet.builder();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                PartitionSpec spec = task.spec();
                StructLike data = task.partition();
                builder.add(new IcebergPartition(spec, data, ChangelogOperation.INSERT));
            }
        } catch (Exception e) {
            LOG.warn("get all iceberg partition failed", e);
        }
        return builder.build();
    }

    // Normalize partition name to yyyy-MM-dd (Type is Date) or yyyy-MM-dd HH:mm:ss (Type is Datetime)
    // Iceberg partition field transform support year, month, day, hour now,
    // eg.
    // year(ts)  partitionName : 2023              return 2023-01-01 (Date) or 2023-01-01 00:00:00 (Datetime)
    // month(ts) partitionName : 2023-01           return 2023-01-01 (Date) or 2023-01-01 00:00:00 (Datetime)
    // day(ts)   partitionName : 2023-01-01        return 2023-01-01 (Date) or 2023-01-01 00:00:00 (Datetime)
    // hour(ts)  partitionName : 2023-01-01-12     return 2023-01-01 12:00:00 (Datetime)
    public static String normalizeTimePartitionName(String partitionName, PartitionField partitionField, Schema schema,
                                                    Type type) {
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        boolean parseFromDate = true;
        IcebergPartitionTransform transform = IcebergPartitionTransform.fromString(partitionField.transform().toString());
        if (transform == IcebergPartitionTransform.YEAR) {
            partitionName += "-01-01";
        } else if (transform == IcebergPartitionTransform.MONTH) {
            partitionName += "-01";
        } else if (transform == IcebergPartitionTransform.DAY) {
            dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        } else if (transform == IcebergPartitionTransform.HOUR) {
            dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH");
            parseFromDate = false;
        } else {
            throw new StarRocksConnectorException("Unsupported partition transform to normalize: %s",
                    partitionField.transform().toString());
        }

        // partition name formatter
        DateTimeFormatter formatter = null;
        if (type.isDate()) {
            formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        } else {
            formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        }
        // If has timestamp with time zone, should compute the time zone offset to UTC
        ZoneId zoneId;
        if (schema.findType(partitionField.sourceId()).equals(Types.TimestampType.withZone())) {
            zoneId = TimeUtils.getTimeZone().toZoneId();
        } else {
            zoneId = ZoneOffset.UTC;
        }

        String result;
        try {
            if (parseFromDate) {
                LocalDate date = LocalDate.parse(partitionName, dateTimeFormatter);
                if (type.isDate()) {
                    result = date.format(formatter);
                } else {
                    LocalDateTime dateTime = date.atStartOfDay().atZone(ZoneOffset.UTC).
                            withZoneSameInstant(zoneId).toLocalDateTime();
                    result = dateTime.format(formatter);
                }
            } else {
                // parse from datetime which contains hour
                LocalDateTime dateTime = LocalDateTime.parse(partitionName, dateTimeFormatter).atZone(ZoneOffset.UTC).
                        withZoneSameInstant(zoneId).toLocalDateTime();
                result = dateTime.format(formatter);
            }
        } catch (Exception e) {
            LOG.warn("parse partition name failed, partitionName: {}, partitionField: {}, type: {}",
                    partitionName, partitionField, type);
            throw new StarRocksConnectorException("parse/format partition name failed", e);
        }
        return result;
    }

    // Get the date interval from iceberg partition transform
    public static PartitionUtil.DateTimeInterval getDateTimeIntervalFromIceberg(IcebergTable table,
                                                                                Column partitionColumn) {
        PartitionField partitionField = table.getPartitionFiled(partitionColumn.getName());
        if (partitionField == null) {
            throw new StarRocksConnectorException("Partition column %s not found in table %s.%s.%s",
                    partitionColumn.getName(), table.getCatalogName(), table.getRemoteDbName(), table.getRemoteTableName());
        }
        String transform = partitionField.transform().toString();
        IcebergPartitionTransform icebergPartitionTransform = IcebergPartitionTransform.fromString(transform);
        switch (icebergPartitionTransform) {
            case YEAR:
                return PartitionUtil.DateTimeInterval.YEAR;
            case MONTH:
                return PartitionUtil.DateTimeInterval.MONTH;
            case DAY:
                return PartitionUtil.DateTimeInterval.DAY;
            case HOUR:
                return PartitionUtil.DateTimeInterval.HOUR;
            default:
                return PartitionUtil.DateTimeInterval.NONE;
        }
    }
}