--funzioni per acquisire i valori dei raster in corrispondenza delle centraline
--19 giugno 2018
--per invcoare la funzione: SELECT * FROM rgriglia.get_raster_value('t2m',1);
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
		St_value(' || quote_ident(parametro) || '.rast,'|| banda || ',c.geom),
		geom
	FROM
		rgriglia.' || quote_ident(parametro) || ',
		centraline.pm10_con_dati 
	WHERE 
		St_intersects(geom,' || quote_ident(parametro) || '.rast);';
END; $$
LANGUAGE 'plpgsql';



--6 luglio 2018
--funzioni per acquisire i valori dei raster in corrispondenza delle centraline
-- A differenza della funzione precedente questa funzione estrae per una determinata banda tutti i valori temporali dei raster sul database postgis
--per invocare la funzione: SELECT * FROM rgriglia.get_raster_value(1);
CREATE OR REPLACE FUNCTION rgriglia.get_raster_values(banda integer)
RETURNS TABLE (
	id_centralina varchar(80),
	idcell integer,
	geom geometry,
	aod550 double precision,
	dust double precision,
	ndvi double precision,
	pbl00 double precision,
	pbl12 double precision,
	sp double precision,
	t2m double precision,
        tp double precision,
	u10 double precision,
	v10 double precision
) AS $$
BEGIN
	RETURN QUERY 

	SELECT 
		c.id_centralina,
		c.idcell,
		c.geom,
		St_Value(aod550.rast,banda,c.geom),
		St_Value(dust.rast,banda,c.geom),
		St_Value(ndvi.rast,banda,c.geom),
		St_Value(pbl00.rast,banda,c.geom),
		St_Value(pbl12.rast,banda,c.geom),
		St_Value(sp.rast,banda,c.geom),
		St_Value(t2m.rast,banda,c.geom),
		St_Value(tp.rast,banda,c.geom),
		St_Value(u10.rast,banda,c.geom),
		St_Value(v10.rast,banda,c.geom)
		
	FROM
		rgriglia.aod550 aod550,
		rgriglia.dust dust,
		rgriglia.ndvi ndvi,
		rgriglia.pbl00 pbl00,
		rgriglia.pbl12 pbl12,
		rgriglia.sp sp,
		rgriglia.t2m t2m,
		rgriglia.tp tp,
		rgriglia.u10 u10,
		rgriglia.v10 v10,
		centraline.pm10_con_dati c
	WHERE 
		(
			St_intersects(aod550.rast,banda,c.geom) AND
			St_intersects(dust.rast,banda,c.geom) AND
			St_intersects(ndvi.rast,banda,c.geom) AND
			St_intersects(pbl00.rast,banda,c.geom) AND
			St_intersects(pbl12.rast,banda,c.geom) AND
			St_intersects(sp.rast,banda,c.geom) AND
			St_intersects(t2m.rast,banda,c.geom) AND
			St_intersects(tp.rast,banda,c.geom) AND
			St_intersects(u10.rast,banda,c.geom) AND
			St_intersects(v10.rast,banda,c.geom)
		);
END; $$
LANGUAGE 'plpgsql';

