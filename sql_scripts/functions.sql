CREATE OR REPLACE PROCEDURE public.insert_f_tickets()
 LANGUAGE plpgsql
AS $procedure$
BEGIN

    INSERT INTO f_tickets (sk_guild, sk_player, sk_time, tickets)
    SELECT 
        COALESCE(dg.id, -1) AS sk_guild,
        COALESCE(dp.id, -1) AS sk_player,
        COALESCE(dt.id, -1) AS sk_time,
        stg.tickets
    FROM 
        stg_tickets stg
    LEFT JOIN 
        d_guild dg ON stg.guild_id = dg.guild_id
    LEFT JOIN 
        d_player dp ON stg.player_id = dp.player_id AND dp.current_flag = TRUE
    LEFT JOIN 
        d_time dt ON stg.date = dt.id;
END $procedure$
;

CREATE OR REPLACE PROCEDURE public.insert_guilds()
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    INSERT INTO d_guild (guild_id, name, guild_reset)
    SELECT stg.guild_id, stg.name, cast(stg.guild_reset as time)
    FROM stg_guild stg
    LEFT JOIN d_guild dim ON stg.guild_id = dim.guild_id
    WHERE dim.guild_id IS NULL;
END;
$procedure$
;


CREATE OR REPLACE PROCEDURE public.upsert_all_players()
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    UPDATE d_player
    SET end_date = CURRENT_DATE - INTERVAL '1 day',
        current_flag = FALSE
    WHERE player_id IN (
        SELECT player_id
        FROM stg_player
    )
    AND current_flag = TRUE
    AND name <> (
        SELECT name
        FROM stg_player
        WHERE stg_player.player_id = d_player.player_id
    );

    INSERT INTO d_player (player_id, name, allycode, start_date, end_date, current_flag)
    SELECT player_id, name, allycode, CURRENT_DATE, NULL, TRUE
    FROM stg_player
    ON CONFLICT (player_id, start_date) DO NOTHING;
END;
$procedure$
;