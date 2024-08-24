CREATE TABLE public.d_time (
	id int8 NOT NULL,
	"date" timestamp NULL,
	"year" int4 NULL,
	quarter int4 NULL,
	"month" int4 NULL,
	day_of_month int4 NULL,
	abbr_day_name text NULL,
	day_name text NULL,
	day_of_week int4 NULL,
	is_weekend bool NULL,
	CONSTRAINT d_time_pk PRIMARY KEY (id)
);


CREATE TABLE public.d_player (
	id serial4 NOT NULL,
	player_id varchar(50) NULL,
	"name" varchar(255) NULL,
	allycode varchar(50) NULL,
	start_date date NULL,
	end_date date NULL,
	current_flag bool NULL,
	CONSTRAINT d_player_pkey PRIMARY KEY (id),
	CONSTRAINT d_player_player_id_start_date_key UNIQUE (player_id, start_date)
);


CREATE TABLE public.d_guild (
	id serial4 NOT NULL,
	guild_id varchar(255) NOT NULL,
	"name" varchar(255) NOT NULL,
	guild_reset time NOT NULL,
	CONSTRAINT d_guild_guild_id_key UNIQUE (guild_id),
	CONSTRAINT d_guild_pkey PRIMARY KEY (id)
);

CREATE TABLE public.f_tickets (
	sk_guild int4 NOT NULL,
	sk_player int4 NOT NULL,
	sk_time int4 NOT NULL,
	tickets int4 NOT NULL,
	CONSTRAINT f_tickets_pkey PRIMARY KEY (sk_guild, sk_player, sk_time),
	CONSTRAINT f_tickets_sk_guild_fkey FOREIGN KEY (sk_guild) REFERENCES public.d_guild(id),
	CONSTRAINT f_tickets_sk_player_fkey FOREIGN KEY (sk_player) REFERENCES public.d_player(id),
	CONSTRAINT f_tickets_sk_time_fkey FOREIGN KEY (sk_time) REFERENCES public.d_time(id)
);