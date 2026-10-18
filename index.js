require('dotenv').config();

const { Readable } = require('node:stream');
const {
  Client,
  GatewayIntentBits,
  Events,
  ChannelType,
  SlashCommandBuilder,
  MessageFlags,
} = require('discord.js');
const {
  joinVoiceChannel,
  getVoiceConnection,
  VoiceConnectionStatus,
  entersState,
  createAudioPlayer,
  createAudioResource,
  StreamType,
  NoSubscriberBehavior,
  AudioPlayerStatus,
} = require('@discordjs/voice');

const {
  DISCORD_TOKEN,
  GUILD_ID,
  VOICE_CHANNEL_ID,
  KEEP_ALIVE_AUDIO = 'true',
} = process.env;

if (!DISCORD_TOKEN) {
  console.error('[startup] DISCORD_TOKEN is missing. Copy .env.example to .env and fill it in.');
  process.exit(1);
}

const PLAY_KEEP_ALIVE = KEEP_ALIVE_AUDIO.toLowerCase() !== 'false';
const WATCHDOG_INTERVAL_MS = 15_000;
const RECONNECT_DELAY_MS = 3_000;
const READY_TIMEOUT_MS = 30_000;
const JOINABLE_TYPES = new Set([ChannelType.GuildVoice, ChannelType.GuildStageVoice]);

// The channel the bot must always be sitting in. `null` means "not pinned yet".
let stickyChannelId = VOICE_CHANNEL_ID ? String(VOICE_CHANNEL_ID).trim() : null;
let reconnectTimer = null;

const client = new Client({
  intents: [GatewayIntentBits.Guilds, GatewayIntentBits.GuildVoiceStates],
});

/**
 * Endless stream of silent 48kHz stereo s16le PCM (one 20ms frame per read).
 * Feeding this to the player keeps the voice connection "active" so Discord
 * never treats the bot as an idle listener.
 */
const SILENT_FRAME = Buffer.alloc(3840);

class SilenceStream extends Readable {
  _read() {
    this.push(SILENT_FRAME);
  }
}

const player = createAudioPlayer({
  behaviors: { noSubscriber: NoSubscriberBehavior.Play },
});

player.on('error', (error) => {
  console.error('[audio] Player error:', error.message);
  scheduleReconnect(1_000);
});

player.on(AudioPlayerStatus.Idle, () => {
  // The silence stream never ends on its own, so Idle means the pipe broke.
  if (!stickyChannelId) return;
  scheduleReconnect(1_000);
});

client.on(Events.Error, (error) => console.error('[client] Error:', error.message));
client.on(Events.Warn, (message) => console.warn('[client] Warn:', message));
process.on('unhandledRejection', (reason) => console.error('[process] Unhandled rejection:', reason));

function ensurePlayer(connection) {
  if (!PLAY_KEEP_ALIVE) return;
  if (connection.state.status === VoiceConnectionStatus.Destroyed) return;

  connection.subscribe(player);

  if (player.state.status === AudioPlayerStatus.Idle) {
    player.play(createAudioResource(new SilenceStream(), { inputType: StreamType.Raw }));
  }
}

function scheduleReconnect(delay = RECONNECT_DELAY_MS) {
  if (!stickyChannelId || reconnectTimer) return;

  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    ensureConnected().catch((error) => console.error('[voice] Reconnect failed:', error.message));
  }, delay);
}

async function connectToChannel(channel) {
  const connection = joinVoiceChannel({
    channelId: channel.id,
    guildId: channel.guild.id,
    adapterCreator: channel.guild.voiceAdapterCreator,
    // Deafened so the bot does not receive audio, but NOT muted so it can
    // still transmit the silence keep-alive track.
    selfDeaf: true,
    selfMute: false,
  });

  connection.on('error', (error) => console.error('[voice] Connection error:', error.message));

  connection.on(VoiceConnectionStatus.Ready, () => {
    console.log(`[voice] Connected to #${channel.name} (${channel.id})`);
    ensurePlayer(connection);
  });

  connection.on(VoiceConnectionStatus.Disconnected, async () => {
    console.warn('[voice] Disconnected — trying to recover...');
    try {
      // A short disconnect usually means Discord is moving us to a new voice server.
      await Promise.race([
        entersState(connection, VoiceConnectionStatus.Signalling, 5_000),
        entersState(connection, VoiceConnectionStatus.Connecting, 5_000),
      ]);
      console.warn('[voice] Transport is recovering...');
    } catch {
      console.warn('[voice] Could not recover — rebuilding the connection.');
      connection.destroy();
      scheduleReconnect();
    }
  });

  try {
    await entersState(connection, VoiceConnectionStatus.Ready, READY_TIMEOUT_MS);
  } catch (error) {
    console.error('[voice] Failed to become ready:', error.message);
    connection.destroy();
    scheduleReconnect();
  }

  return connection;
}

async function ensureConnected() {
  if (!stickyChannelId) return null;

  let channel = client.channels.cache.get(stickyChannelId);
  if (!channel) {
    try {
      channel = await client.channels.fetch(stickyChannelId);
    } catch {
      return null; // Channel deleted or temporarily unavailable.
    }
  }

  if (!channel || !JOINABLE_TYPES.has(channel.type)) return null;

  const existing = getVoiceConnection(channel.guild.id);
  if (existing) {
    if (existing.joinConfig.channelId === channel.id) {
      const status = existing.state.status;
      if (status === VoiceConnectionStatus.Ready) {
        ensurePlayer(existing);
        return existing;
      }
      if (
        status === VoiceConnectionStatus.Connecting ||
        status === VoiceConnectionStatus.Signalling
      ) {
        return existing;
      }
    }
    existing.destroy();
  }

  return connectToChannel(channel);
}

async function registerCommands() {
  const body = [
    new SlashCommandBuilder()
      .setName('join')
      .setDescription('Join your current voice channel and stay there 24/7'),
    new SlashCommandBuilder()
      .setName('leave')
      .setDescription('Stop holding the voice channel and disconnect'),
    new SlashCommandBuilder()
      .setName('voice-status')
      .setDescription('Show the current voice connection status'),
  ].map((command) => command.toJSON());

  try {
    if (GUILD_ID) {
      const guild = await client.guilds.fetch(GUILD_ID);
      await guild.commands.set(body);
      console.log(`[commands] Registered ${body.length} commands to guild ${GUILD_ID}`);
    } else {
      await client.application.commands.set(body);
      console.log(`[commands] Registered ${body.length} global commands`);
    }
  } catch (error) {
    console.error('[commands] Registration failed:', error.message);
  }
}

client.once(Events.ClientReady, async (readyClient) => {
  console.log(`[bot] Logged in as ${readyClient.user.tag}`);

  await registerCommands();

  if (stickyChannelId) {
    await ensureConnected();
  } else {
    console.log('[bot] No VOICE_CHANNEL_ID configured — use /join inside a voice channel.');
  }
});

client.on(Events.VoiceStateUpdate, (oldState, newState) => {
  if (!client.user || newState.id !== client.user.id) return;
  if (!stickyChannelId) return;

  // Someone moved or kicked the bot out of its pinned channel.
  if (newState.channelId !== stickyChannelId) {
    console.warn('[voice] Bot left its pinned channel — rejoining...');
    scheduleReconnect();
  }
});

client.on(Events.GuildUnavailable, (guild) => {
  console.warn(`[voice] Guild ${guild.id} is unavailable — will rejoin once it is back.`);
  scheduleReconnect(5_000);
});

client.on(Events.InteractionCreate, async (interaction) => {
  if (!interaction.isChatInputCommand() || !interaction.guild) return;

  if (interaction.commandName === 'join') {
    const channel = interaction.member?.voice?.channel;
    if (!channel || !JOINABLE_TYPES.has(channel.type)) {
      await interaction.reply({
        content: 'Join a voice channel first, then run `/join` again.',
        flags: MessageFlags.Ephemeral,
      });
      return;
    }

    stickyChannelId = channel.id;
    await interaction.reply({
      content: `Locked onto ${channel} — I will stay there and reconnect automatically.`,
      flags: MessageFlags.Ephemeral,
    });
    await ensureConnected();
    return;
  }

  if (interaction.commandName === 'leave') {
    stickyChannelId = null;
    if (reconnectTimer) {
      clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }
    const connection = getVoiceConnection(interaction.guild.id);
    if (connection) connection.destroy();
    await interaction.reply({ content: 'Disconnected.', flags: MessageFlags.Ephemeral });
    return;
  }

  if (interaction.commandName === 'voice-status') {
    const connection = getVoiceConnection(interaction.guild.id);
    const current = connection?.joinConfig.channelId ?? null;
    const status = connection?.state.status ?? 'not connected';
    await interaction.reply({
      content: [
        `Pinned channel: ${stickyChannelId ? `<#${stickyChannelId}>` : '*none*'}`,
        `Current channel: ${current ? `<#${current}>` : '*none*'}`,
        `Connection state: \`${status}\``,
        `Keep-alive audio: \`${PLAY_KEEP_ALIVE ? 'on' : 'off'}\``,
      ].join('\n'),
      flags: MessageFlags.Ephemeral,
    });
  }
});

// Safety net: every 15s make sure the bot is still where it belongs.
setInterval(() => {
  if (!stickyChannelId) return;
  ensureConnected().catch((error) => console.error('[voice] Watchdog failed:', error.message));
}, WATCHDOG_INTERVAL_MS);

client.login(DISCORD_TOKEN).catch((error) => {
  console.error('[startup] Login failed:', error.message);
  process.exit(1);
});
