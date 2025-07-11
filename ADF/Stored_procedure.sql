SELECT * FROM [dbo].[car_source_data];

CREATE TABLE water_table (
last_load VARCHAR(200)
)

SELECT * FROM water_table;

SELECT MIN(DATE_ID) FROM [dbo].[car_source_data];

INSERT INTO water_table VALUES ('DT00000');

CREATE PROCEDURE updateWatermarkTable 
@last_load VARCHAR(200)
AS
BEGIN
    BEGIN TRY
        BEGIN TRANSACTION;

        UPDATE water_table
        SET LAST_LOAD = @last_load;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        ROLLBACK TRANSACTION;

        -- Optional: Throw error back to caller
        THROW;
    END CATCH
END;


DROP PROCEDURE updateWatermarkTable;
