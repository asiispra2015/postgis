-- Luglio 2018

--aggiunta dei dati delle emissioni prodotti da Ernesto
-- Primo tentativo: creare colonne pm10_diff, nh3_diffus e co_punctual su vgriglia.centroidi e fare un update delle colonne mediante la tabella vgriglia.emissioni;
-- Troppo lento!!!!!

-- Secondo tentativo: creare tabella temporanea centroidi2 (operazione velocissima con valori dei centroidi e valori delle emissioni) e ricreare  tutti i constraints.	

BEGIN;

DROP TABLE IF EXISTS centroidi2;

CREATE TABLE centroidi2 AS (

      SELECT c.*,e.pm10_diffu,e.nh3_diffus,e.co_puntual
      FROM vgriglia.emissioni e, vgriglia.centroidi c
      WHERE St_intersects(c.geom,e.geom)

);


ALTER TABLE centroidi2
  ADD CONSTRAINT centroidi_pkey2 PRIMARY KEY(gid);

ALTER TABLE centroidi2
  ADD CONSTRAINT unique_idcell2 UNIQUE(idcell);

ALTER TABLE centroidi2 SET SCHEMA vgriglia;

-- create index
CREATE INDEX centroidi2_geom_idx
  ON vgriglia.centroidi2
  USING gist
  (geom);

ALTER TABLE centraline.pm10 DROP CONSTRAINT centroidi_id_contraint;

ALTER TABLE centraline.pm10
  ADD CONSTRAINT centroidi2_id_contraint FOREIGN KEY (idcell)
      REFERENCES vgriglia.centroidi2 (idcell) MATCH SIMPLE
      ON UPDATE CASCADE ON DELETE NO ACTION;

--elimino centroidi
DROP TABLE vgriglia.centroidi;

--nuovo centroidi
ALTER TABLE vgriglia.centroidi2 RENAME TO centroidi;

COMMIT;

