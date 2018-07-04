--aggiunta delle variabili calcolate come buffer utilizzando osm

BEGIN;

DROP TABLE IF EXISTS buf_osm;

--crea tabella con i centroidi di vgriglia.centroidi e le variabili di osm
CREATE TABLE centroidi2 AS (

	SELECT 
		c.*, 
		ST_Value(a1.rast,c.geom) AS avg_buf_a1,
		ST_Value(a23.rast,c.geom) AS avg_buf_a23,
		ST_Value(oth.rast,c.geom) AS avg_buf_oth
   
	FROM 
		vgriglia.centroidi c, rgriglia.avg_buf_a1 a1, rgriglia.avg_buf_a23 a23, rgriglia.avg_buf_oth oth
	WHERE  
		(St_Intersects(a1.rast,c.geom) AND St_Intersects(a23.rast,c.geom) AND St_Intersects(oth.rast,c.geom))

); 

--elimino il constraint su centraline.pm10
ALTER TABLE centraline.pm10 DROP CONSTRAINT centroidi2_id_contraint;

--elimino centroidi
DROP TABLE vgriglia.centroidi;

ALTER TABLE centroidi2
  ADD CONSTRAINT centroidi_pkey PRIMARY KEY(gid);

ALTER TABLE centroidi2
  ADD CONSTRAINT unique_idcell UNIQUE(idcell);

ALTER TABLE centroidi2 SET SCHEMA vgriglia;

-- create index
CREATE INDEX centroidi_geom_idx
  ON vgriglia.centroidi2
  USING gist
  (geom);

ALTER TABLE centraline.pm10
  ADD CONSTRAINT centroidi_id_contraint FOREIGN KEY (idcell)
      REFERENCES vgriglia.centroidi2 (idcell) MATCH SIMPLE
      ON UPDATE CASCADE ON DELETE NO ACTION;

--nuovo centroidi
ALTER TABLE vgriglia.centroidi2 RENAME TO centroidi;


COMMIT;
