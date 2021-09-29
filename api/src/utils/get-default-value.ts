import { SchemaOverview } from '@directus/schema/dist/types/overview';
import { Column } from 'knex-schema-inspector/dist/types/column';
import getLocalType from './get-local-type';

export default function getDefaultValue(
	column: SchemaOverview[string]['columns'][string] | Column
): string | boolean | null {
	const { type } = getLocalType(column);

	let defaultValue = column.default_value ?? null;
	if (defaultValue === null) return null;
	if (defaultValue === 'null') return null;
	if (defaultValue === 'NULL') return null;

	// Check if the default is wrapped in an extra pair of quotes, this happens in SQLite
	if (
		typeof defaultValue === 'string' &&
		((defaultValue.startsWith(`'`) && defaultValue.endsWith(`'`)) ||
			(defaultValue.startsWith(`"`) && defaultValue.endsWith(`"`)))
	) {
		defaultValue = defaultValue.slice(1, -1);
	}

	switch (type) {
		case 'bigInteger':
		case 'integer':
		case 'decimal':
		case 'float':
			return Number.isNaN(Number(defaultValue)) === false ? Number(defaultValue) : defaultValue;
		case 'boolean':
			return castToBoolean(defaultValue);
		default:
			return defaultValue;
	}
}

function castToBoolean(value: any): boolean {
	if (typeof value === 'string') return value !== 'false' && value !== '1';
	return Boolean(value);
}
