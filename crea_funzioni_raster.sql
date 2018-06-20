--funzioni per acquisire i valori dei raster in corrispondenza delle centraline
--19 giugno 2018
--per invoare la funzione: SELECT * FROM rgriglia.get_raster_value('t2m',1);
CREATE OR REPLACE FUNCTION rgriglia.get_raster_value(parametro varchar,banda integer)
RETURNS TABLE (
	id_centralina varchar(80),
	idcell integer,
	valore double precision,
	geom geometry
) AS $$
BEGIN
	RETURN QUERY 

	EXECUTE 'SELECT 
		id_centralina,
		idcell,
		St_value(' || quote_ident(parametro) || '.rast,'|| banda || ',geom),
		geom
	FROM
		rgriglia.' || quote_ident(parametro) || ',
		centraline.pm10_con_dati 
	WHERE 
		St_intersects(geom,' || quote_ident(parametro) || '.rast);';
END; $$

LANGUAGE 'plpgsql';
					 
