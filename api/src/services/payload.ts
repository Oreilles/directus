import { format, parseISO } from 'date-fns';
import Joi from 'joi';
import { Knex } from 'knex';
import { clone, cloneDeep, isObject, isPlainObject, omit, isNil, identity } from 'lodash';
import { v4 as uuidv4 } from 'uuid';
import getDatabase from '../database';
import { ForbiddenException, InvalidPayloadException } from '../exceptions';
import { AbstractServiceOptions, Item, PrimaryKey, Query, SchemaOverview, Alterations } from '../types';
import { Accountability } from '@directus/shared/types';
import { toArray } from '@directus/shared/utils';
import { ItemsService } from './items';
import { getGeometryHelper } from '../database/helpers/geometry';
import { parse as wktToGeoJSON } from 'wellknown';
import { generateHash } from '../utils/generate-hash';

type Action = 'create' | 'read' | 'update';

type Transformer = {
	read?: (value: any) => any | Promise<any>;
	create?: (value: any) => any | Promise<any>;
	update?: (value: any) => any | Promise<any>;
};
type Transformers = {
	[type: string]: Transformer;
};

/**
 * Process a given payload for a collection to ensure the special fields (hash, uuid, date etc) are
 * handled correctly.
 */
export class PayloadService {
	accountability: Accountability | null;
	knex: Knex;
	collection: string;
	schema: SchemaOverview;
	geoUtil: ReturnType<typeof getGeometryHelper>;

	constructor(collection: string, options: AbstractServiceOptions) {
		this.accountability = options.accountability || null;
		this.knex = options.knex || getDatabase();
		this.collection = collection;
		this.schema = options.schema;
		this.geoUtil = getGeometryHelper(this.knex);

		return this;
	}

	public specialGenerators: Transformers = {
		uuid: {
			create: (value) => value ?? uuidv4(),
		},
		'user-created': {
			create: (_) => this.accountability?.user ?? null,
		},
		'user-updated': {
			update: (_) => this.accountability?.user ?? null,
		},
		'role-created': {
			create: (_) => this.accountability?.role ?? null,
		},
		'role-updated': {
			update: (_) => this.accountability?.role ?? null,
		},
		'date-created': {
			create: (_) => this.knex.raw('CURRENT_TIMESTAMP'),
		},
		'date-updated': {
			update: (_) => this.knex.raw('CURRENT_TIMESTAMP'),
		},
	};

	public specialTransformers: Transformers = {
		hash: {
			create: (value) => generateHash(String(value)),
			update: (value) => generateHash(String(value)),
		},
		conceal: {
			read: (value) => (value ? '**********' : null),
		},
		boolean: {
			read: (value) => value == true,
		},
		csv: {
			read: (value) => toArray(value),
			create: (value) => (Array.isArray(value) ? value.join(',') : value),
			update: (value) => (Array.isArray(value) ? value.join(',') : value),
		},
		json: {
			read: (value) => {
				if (typeof value === 'string') {
					try {
						return JSON.parse(value);
					} catch {
						return value;
					}
				}
				return value;
			},
		},
	};

	public typeTransformers: Transformers = {
		geometry: {
			read: (value) => (typeof value === 'string' ? wktToGeoJSON(value) : value),
			create: (value) => (!value ? value : this.geoUtil.fromGeoJSON(this.specialTransformers.json.read!(value))),
			update: (value) => (!value ? value : this.geoUtil.fromGeoJSON(this.specialTransformers.json.read!(value))),
		},
		date: {
			read: (value) => this.toDate(value)?.toISOString().slice(0, 10),
			create: (value) => value.slice(0, 10),
			update: (value) => value.slice(0, 10),
		},
		dateTime: {
			read: (value) => this.toDate(value)?.toISOString().slice(0, 19),
			create: (value) => this.toDate(value)?.toISOString().slice(0, 19),
			update: (value) => this.toDate(value)?.toISOString().slice(0, 19),
		},
		timestamp: {
			read: (value) => this.toDate(value)?.toISOString(),
			create: (value) => this.toDate(value)?.toISOString(),
			update: (value) => this.toDate(value)?.toISOString(),
		},
		time: {
			read: (value) => this.toDate(value)?.toString().slice(11, 19),
			create: (value) => this.toDate(value)?.toString().slice(11, 19),
			update: (value) => this.toDate(value)?.toString().slice(11, 19),
		},
	};

	toDate(value: number | string | Date): Date | null {
		if (value === '0000-00-00') {
			return null;
		}
		return new Date(value);
	}

	stringifyExceptRaw(value: any) {
		if (!value || value.isRawInstance || value instanceof Date) {
			return value;
		}
		if (typeof value === 'object') {
			return JSON.stringify(value);
		}
		return value;
	}

	processValues(action: Action, payloads: Partial<Item>[]): Promise<Partial<Item>[]>;
	processValues(action: Action, payload: Partial<Item>): Promise<Partial<Item>>;
	async processValues(
		action: Action,
		payload: Partial<Item> | Partial<Item>[]
	): Promise<Partial<Item> | Partial<Item>[]> {
		const processedPayload = toArray(payload);

		const fieldsData = this.schema.collections[this.collection].fields;
		const fields = Object.keys(fieldsData);

		await Promise.all(
			fields.map(async (field: string) => {
				const type = fieldsData[field].type;
				const special = fieldsData[field].special[0];
				const generate = this.specialGenerators[special]?.[action];
				const transformSpecial = this.specialTransformers[special]?.[action];
				const transformType = this.typeTransformers[type]?.[action];

				await Promise.all(
					processedPayload.map(async (record: any) => {
						if (generate) {
							record[field] = generate(record[field]);
						}
						if (record[field] == null) {
							return;
						}
						if (transformSpecial) {
							const value = transformSpecial(record[field]);
							record[field] = value?.isRawInstance ? value : await value;
						}
						if (transformType) {
							record[field] = transformType(record[field]);
						}
						if (action === 'read') {
							record[field] = this.stringifyExceptRaw(record[field]);
						}
					})
				);
			})
		);
		if (Array.isArray(payload)) {
			return processedPayload;
		}
		return processedPayload[0];
	}

	/**
	 * Recursively save/update all nested related Any-to-One items
	 */
	async processA2O(data: Partial<Item>): Promise<{ payload: Partial<Item>; revisions: PrimaryKey[] }> {
		const relations = this.schema.relations.filter((relation) => {
			return relation.collection === this.collection;
		});

		const revisions: PrimaryKey[] = [];

		const payload = cloneDeep(data);

		// Only process related records that are actually in the payload
		const relationsToProcess = relations.filter((relation) => {
			return relation.field in payload && isPlainObject(payload[relation.field]);
		});

		for (const relation of relationsToProcess) {
			// If the required a2o configuration fields are missing, this is a m2o instead of an a2o
			if (!relation.meta?.one_collection_field || !relation.meta?.one_allowed_collections) continue;

			const relatedCollection = payload[relation.meta.one_collection_field];

			if (!relatedCollection) {
				throw new InvalidPayloadException(
					`Can't update nested record "${relation.collection}.${relation.field}" without field "${relation.collection}.${relation.meta.one_collection_field}" being set`
				);
			}

			const allowedCollections = relation.meta.one_allowed_collections;

			if (allowedCollections.includes(relatedCollection) === false) {
				throw new InvalidPayloadException(
					`"${relation.collection}.${relation.field}" can't be linked to collection "${relatedCollection}`
				);
			}

			const itemsService = new ItemsService(relatedCollection, {
				accountability: this.accountability,
				knex: this.knex,
				schema: this.schema,
			});

			const relatedPrimary = this.schema.collections[relatedCollection].primary;
			const relatedRecord: Partial<Item> = payload[relation.field];

			if (['string', 'number'].includes(typeof relatedRecord)) continue;

			const hasPrimaryKey = relatedPrimary in relatedRecord;

			let relatedPrimaryKey: PrimaryKey = relatedRecord[relatedPrimary];

			const exists =
				hasPrimaryKey &&
				!!(await this.knex
					.select(relatedPrimary)
					.from(relatedCollection)
					.where({ [relatedPrimary]: relatedPrimaryKey })
					.first());

			if (exists) {
				const fieldsToUpdate = omit(relatedRecord, relatedPrimary);

				if (Object.keys(fieldsToUpdate).length > 0) {
					await itemsService.updateOne(relatedPrimaryKey, relatedRecord, {
						onRevisionCreate: (id) => revisions.push(id),
					});
				}
			} else {
				relatedPrimaryKey = await itemsService.createOne(relatedRecord, {
					onRevisionCreate: (id) => revisions.push(id),
				});
			}

			// Overwrite the nested object with just the primary key, so the parent level can be saved correctly
			payload[relation.field] = relatedPrimaryKey;
		}

		return { payload, revisions };
	}

	/**
	 * Save/update all nested related m2o items inside the payload
	 */
	async processM2O(data: Partial<Item>): Promise<{ payload: Partial<Item>; revisions: PrimaryKey[] }> {
		const payload = cloneDeep(data);

		// All the revisions saved on this level
		const revisions: PrimaryKey[] = [];

		// Many to one relations that exist on the current collection
		const relations = this.schema.relations.filter((relation) => {
			return relation.collection === this.collection;
		});

		// Only process related records that are actually in the payload
		const relationsToProcess = relations.filter((relation) => {
			return relation.field in payload && isObject(payload[relation.field]);
		});

		for (const relation of relationsToProcess) {
			// If no "one collection" exists, this is a A2O, not a M2O
			if (!relation.related_collection) continue;
			const relatedPrimaryKeyField = this.schema.collections[relation.related_collection].primary;

			// Items service to the related collection
			const itemsService = new ItemsService(relation.related_collection, {
				accountability: this.accountability,
				knex: this.knex,
				schema: this.schema,
			});

			const relatedRecord: Partial<Item> = payload[relation.field];

			if (['string', 'number'].includes(typeof relatedRecord)) continue;

			const hasPrimaryKey = relatedPrimaryKeyField in relatedRecord;

			let relatedPrimaryKey: PrimaryKey = relatedRecord[relatedPrimaryKeyField];

			const exists =
				hasPrimaryKey &&
				!!(await this.knex
					.select(relatedPrimaryKeyField)
					.from(relation.related_collection)
					.where({ [relatedPrimaryKeyField]: relatedPrimaryKey })
					.first());

			if (exists) {
				const fieldsToUpdate = omit(relatedRecord, relatedPrimaryKeyField);

				if (Object.keys(fieldsToUpdate).length > 0) {
					await itemsService.updateOne(relatedPrimaryKey, relatedRecord, {
						onRevisionCreate: (id) => revisions.push(id),
					});
				}
			} else {
				relatedPrimaryKey = await itemsService.createOne(relatedRecord, {
					onRevisionCreate: (id) => revisions.push(id),
				});
			}

			// Overwrite the nested object with just the primary key, so the parent level can be saved correctly
			payload[relation.field] = relatedPrimaryKey;
		}

		return { payload, revisions };
	}

	/**
	 * Recursively save/update all nested related o2m items
	 */
	async processO2M(data: Partial<Item>, parent: PrimaryKey): Promise<{ revisions: PrimaryKey[] }> {
		const revisions: PrimaryKey[] = [];

		const relations = this.schema.relations.filter((relation) => {
			return relation.related_collection === this.collection;
		});

		const payload = cloneDeep(data);

		// Only process related records that are actually in the payload
		const relationsToProcess = relations.filter((relation) => {
			if (!relation.meta?.one_field) return false;
			return relation.meta.one_field in payload;
		});

		const nestedUpdateSchema = Joi.object({
			create: Joi.array().items(Joi.object().unknown()),
			update: Joi.array().items(Joi.object().unknown()),
			delete: Joi.array().items(Joi.string(), Joi.number()),
		});

		for (const relation of relationsToProcess) {
			if (!relation.meta || !payload[relation.meta.one_field!]) continue;

			const currentPrimaryKeyField = this.schema.collections[relation.related_collection!].primary;
			const relatedPrimaryKeyField = this.schema.collections[relation.collection].primary;

			const itemsService = new ItemsService(relation.collection, {
				accountability: this.accountability,
				knex: this.knex,
				schema: this.schema,
			});

			const recordsToUpsert: Partial<Item>[] = [];
			const savedPrimaryKeys: PrimaryKey[] = [];

			// Nested array of individual items
			if (Array.isArray(payload[relation.meta!.one_field!])) {
				for (let i = 0; i < (payload[relation.meta!.one_field!] || []).length; i++) {
					const relatedRecord = (payload[relation.meta!.one_field!] || [])[i];

					let record = cloneDeep(relatedRecord);

					if (typeof relatedRecord === 'string' || typeof relatedRecord === 'number') {
						const existingRecord = await this.knex
							.select(relatedPrimaryKeyField, relation.field)
							.from(relation.collection)
							.where({ [relatedPrimaryKeyField]: record })
							.first();

						if (!!existingRecord === false) {
							throw new ForbiddenException();
						}

						// If the related item is already associated to the current item, and there's no
						// other updates (which is indicated by the fact that this is just the PK, we can
						// ignore updating this item. This makes sure we don't trigger any update logic
						// for items that aren't actually being updated. NOTE: We use == here, as the
						// primary key might be reported as a string instead of number, coming from the
						// http route, and or a bigInteger in the DB
						if (
							isNil(existingRecord[relation.field]) === false &&
							(existingRecord[relation.field] == parent ||
								existingRecord[relation.field] == payload[currentPrimaryKeyField])
						) {
							savedPrimaryKeys.push(existingRecord[relatedPrimaryKeyField]);
							continue;
						}

						record = {
							[relatedPrimaryKeyField]: relatedRecord,
						};
					}

					recordsToUpsert.push({
						...record,
						[relation.field]: parent || payload[currentPrimaryKeyField],
					});
				}

				savedPrimaryKeys.push(
					...(await itemsService.upsertMany(recordsToUpsert, {
						onRevisionCreate: (id) => revisions.push(id),
					}))
				);

				const query: Query = {
					filter: {
						_and: [
							{
								[relation.field]: {
									_eq: parent,
								},
							},
							{
								[relatedPrimaryKeyField]: {
									_nin: savedPrimaryKeys,
								},
							},
						],
					},
				};

				// Nullify all related items that aren't included in the current payload
				if (relation.meta.one_deselect_action === 'delete') {
					// There's no revision for a deletion
					await itemsService.deleteByQuery(query);
				} else {
					await itemsService.updateByQuery(
						query,
						{ [relation.field]: null },
						{
							onRevisionCreate: (id) => revisions.push(id),
						}
					);
				}
			}
			// "Updates" object w/ create/update/delete
			else {
				const alterations = payload[relation.meta!.one_field!] as Alterations;
				const { error } = nestedUpdateSchema.validate(alterations);
				if (error) throw new InvalidPayloadException(`Invalid one-to-many update structure: ${error.message}`);

				if (alterations.create) {
					await itemsService.createMany(
						alterations.create.map((item) => ({
							...item,
							[relation.field]: parent || payload[currentPrimaryKeyField],
						})),
						{
							onRevisionCreate: (id) => revisions.push(id),
						}
					);
				}

				if (alterations.update) {
					const primaryKeyField = this.schema.collections[relation.collection].primary;

					for (const item of alterations.update) {
						await itemsService.updateOne(
							item[primaryKeyField],
							{
								...item,
								[relation.field]: parent || payload[currentPrimaryKeyField],
							},
							{
								onRevisionCreate: (id) => revisions.push(id),
							}
						);
					}
				}

				if (alterations.delete) {
					const query: Query = {
						filter: {
							_and: [
								{
									[relation.field]: {
										_eq: parent,
									},
								},
								{
									[relatedPrimaryKeyField]: {
										_in: alterations.delete,
									},
								},
							],
						},
					};

					if (relation.meta.one_deselect_action === 'delete') {
						await itemsService.deleteByQuery(query);
					} else {
						await itemsService.updateByQuery(
							query,
							{ [relation.field]: null },
							{
								onRevisionCreate: (id) => revisions.push(id),
							}
						);
					}
				}
			}
		}

		return { revisions };
	}

	/**
	 * Transforms the input partial payload to match the output structure, to have consistency
	 * between delta and data
	 */
	async prepareDelta(data: Partial<Item>): Promise<string> {
		let payload = cloneDeep(data);

		for (const key in payload) {
			if (payload[key]?.isRawInstance) {
				payload[key] = payload[key].bindings[0];
			}
		}

		payload = await this.processValues('read', payload);

		return JSON.stringify(payload);
	}
}
