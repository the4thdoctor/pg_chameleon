SELECT 
	pg_catalog.format_type(a.atttypid, a.atttypmod),
	(
		SELECT 
			split_part(substring(pg_catalog.pg_get_expr(d.adbin, d.adrelid) for 128),'::',1)
		FROM 
			pg_catalog.pg_attrdef d
		WHERE 
				d.adrelid = a.attrelid 
			AND d.adnum = a.attnum 
			AND a.atthasdef
	) as default_value,
	(
		SELECT 
			pg_catalog.pg_get_expr(d.adbin, d.adrelid)
		FROM 
			pg_catalog.pg_attrdef d
		WHERE 
				d.adrelid = a.attrelid 
			AND d.adnum = a.attnum 
			AND a.atthasdef
	) as full_definition,
	*
	FROM 
		pg_catalog.pg_attribute a
	WHERE 
			a.attrelid = 'sch_chameleon.t_sources'::regclass 
		AND a.attname='enm_status' 
		AND NOT a.attisdropped
;
