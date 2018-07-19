-- annullo tutti i campi del corine land cover
UPDATE vgriglia.centroidi SET clc_agri=0;
UPDATE vgriglia.centroidi SET clc_arbl=0;
UPDATE vgriglia.centroidi SET clc_crop=0;
UPDATE vgriglia.centroidi SET clc_dcds=0;
UPDATE vgriglia.centroidi SET clc_evgr=0;
UPDATE vgriglia.centroidi SET clc_hidv=0;
UPDATE vgriglia.centroidi SET clc_lwdv=0;
UPDATE vgriglia.centroidi SET clc_pstr=0;
UPDATE vgriglia.centroidi SET clc_shrb=0;

-- Poligoni del corine land cover:
-- all'interno di ogni cella della griglia è possibile avere uno o più poligono del Corine Land Cover riferiti a uno stesso campo (ad esempio crop)
-- quindi: 1) i poligono di uno stesso campo (ad esempio crop) vanno uniti (ST_union) e le corrispettive aree sommate
-- 2) Il risultato di St_union (la tabella z nella funzione che segue) va confrontato con vgriglia.centroidi. Il confronto va fatto con St_Contains e NON
-- con St_Intersects (una cella di griglia interseca non solo la il poligono che ricopre ma anche i poligoni limitrofi che toccano la cella!!)
-- I valori dei poligoni del CLC assegnati alla cella di griglia vengono poi assegnati ai centroidi

-- PERCHÈ DEVO LAVORARE CON  vgriglia.griglia INVECE CHE DIRETTAMENTE CON vgriglia.centroidi? PERCHè i centroidi non necessariamente toccano i poligoni
-- del Corine Land Cover all'interno della cella corrispondente a ciascun centroide!

--funzione che aggrega all'interno di ogni cella della griglia i poligono e ne somma le aree
CREATE OR REPLACE FUNCTION vgriglia.update_clc(campo varchar) RETURNS VOID AS
$$
BEGIN

	DROP TABLE IF EXISTS vgriglia.appo;
	DROP TABLE IF EXISTS vgriglia.z;

	CREATE TABLE vgriglia.appo AS (

		SELECT
		
		geom,
		gid,
		0 AS perc

		FROM

		vgriglia.griglia

	);

	CREATE INDEX appo_idx ON vgriglia.appo USING gist(geom);

	EXECUTE format('CREATE TABLE vgriglia.z AS (

		Select St_Union(sub.geom) AS geom,SUM(sub.area)/10000 AS perc,id  FROM (
			SELECT clc.area,clc.geom,aa.gid as id FROM vgriglia.clc_%s clc,vgriglia.appo aa WHERE St_Contains(aa.geom,clc.geom)
		) sub GROUP BY sub.id

	);',campo);

	CREATE INDEX z_idx ON vgriglia.z USING gist(geom);

	UPDATE vgriglia.appo SET perc=ROUND(vgriglia.z.perc) FROM vgriglia.z WHERE St_contains(vgriglia.appo.geom,vgriglia.z.geom);

	EXECUTE format('UPDATE vgriglia.centroidi SET clc_%s=appo.perc FROM  vgriglia.appo appo WHERE St_Contains(appo.geom,vgriglia.centroidi.geom)',campo);

	
	RETURN;

END;
$$ LANGUAGE plpgsql;

--update dei vari campi
SELECT * FROM vgriglia.update_clc('agri');
SELECT * FROM vgriglia.update_clc('arbl');
SELECT * FROM vgriglia.update_clc('crop');
SELECT * FROM vgriglia.update_clc('dcds');
SELECT * FROM vgriglia.update_clc('evgr');
SELECT * FROM vgriglia.update_clc('hidv');
SELECT * FROM vgriglia.update_clc('lwdv');
SELECT * FROM vgriglia.update_clc('shrb');
SELECT * FROM vgriglia.update_clc('pstr');
