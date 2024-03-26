import { newDeltaTable, type JsDeltaTable } from "..";

export class DeltaTable {
	#table: JsDeltaTable;
	/**
	 * Create the Delta Table from a path with an optional version.
	 * Multiple StorageBackends are currently supported: AWS S3, Azure Data Lake Storage Gen2, Google Cloud Storage (GCS) and local URI.
	 * Depending on the storage backend used, you could provide options values using the `storage_options` parameter.
	 *
	 * @param table_uri - the path of the DeltaTable
	 * @param version - version of the DeltaTable
	 * @param storage_options - a dictionary of the options to use for the storage backend
	 * @returns A new `DeltaTable` instance
	 */
	constructor(tableUri: string, version?: number) {
		this.#table = newDeltaTable(tableUri, version);
	}
  static fromDataCatalog
}
