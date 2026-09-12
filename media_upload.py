"""Stage source media and build explicit MTProto sends without forwarding headers.
Staging uploads media objects only; it never posts a destination message.
"""
import copy
import mimetypes
from pathlib import Path
import shutil
import time

from telethon import functions, types, utils


class UploadProblem(Exception):
    pass


class UploadStopped(UploadProblem):
    pass


def normalize_thumbnail(path):
    """Keep a valid source JPEG unchanged; normalize only to Telegram's limits."""
    from PIL import Image
    path = Path(path)
    with Image.open(path) as im:
        if im.format == 'JPEG' and max(im.size) <= 320 and path.stat().st_size < 200_000:
            return path
        image = im.convert('RGB')
        image.thumbnail((320, 320))
        target = path.with_suffix('.normalized.jpg')
        image.save(target, 'JPEG', quality=90, optimize=True)
    return target


def check_space(messages, directory):
    needed = sum(getattr(getattr(m.media, 'document', None), 'size', 0) for m in messages)
    if shutil.disk_usage(directory).free < needed + 50 * 1024 * 1024:
        raise UploadProblem('Insufficient disk space for the next album plus 50 MiB of working space.')


def validate_album(messages):
    if len(messages) > 10:
        raise UploadProblem('Telegram upload albums allow at most 10 items; refusing to split the source album.')
    if len(messages) > 1:
        groups = {m.grouped_id for m in messages}
        if len(groups) != 1 or None in groups:
            raise UploadProblem('Source album membership changed since scanning. No destination messages were posted.')
        if any(not isinstance(m.media, (types.MessageMediaPhoto, types.MessageMediaDocument)) for m in messages):
            raise UploadProblem('Unsupported item inside an album; refusing to split it.')
        if len({bool(getattr(m, 'invert_media', False)) for m in messages}) > 1:
            raise UploadProblem('Album has mixed caption positions which one upload request cannot preserve.')


class Stager:
    def __init__(self, client, destination, directory, log, stopped=lambda: False):
        self.client = client
        self.destination = destination
        self.directory = Path(directory)
        self.log = log
        self.stopped = stopped
        self.last_progress = 0

    def progress(self, current, total):
        self.check_stop()
        now = time.monotonic()
        if now - self.last_progress >= 10:
            self.log(f'Transferring file: {current / 1048576:.1f}/{total / 1048576:.1f} MiB')
            self.last_progress = now

    def check_stop(self):
        if self.stopped():
            raise UploadStopped('Stopped before posting; current source unit will be retried on restart.')

    async def upload_path(self, path):
        self.check_stop()
        return await self.client.upload_file(str(path), progress_callback=self.progress)

    async def source_thumbnail(self, message):
        thumbs = getattr(message.media.document, 'thumbs', None) or []
        available = [(i, t) for i, t in enumerate(thumbs)
                     if isinstance(t, (types.PhotoSize, types.PhotoCachedSize, types.PhotoSizeProgressive))]
        if not available:
            self.log(f'ID {message.id}: no downloadable original thumbnail; Telegram may generate a preview.')
            return None
        _, original = max(available, key=lambda pair: pair[1].w * pair[1].h)
        # Telethon sorts thumbnails internally, so a source-list numeric index is
        # not stable. The Telegram thumbnail type selects the intended image.
        path = await self.client.download_media(message, file=str(self.directory / f'{message.id}-thumb.jpg'), thumb=original.type)
        if not path:
            raise UploadProblem(f'ID {message.id}: original thumbnail could not be downloaded. No post was sent.')
        return await self.upload_path(normalize_thumbnail(path))

    async def source_cover(self, message):
        cover = getattr(message.media, 'video_cover', None)
        if not isinstance(cover, types.Photo):
            return None
        path = await self.client.download_media(cover, file=str(self.directory / f'{message.id}-cover.jpg'))
        if not path:
            raise UploadProblem(f'ID {message.id}: source video cover could not be downloaded.')
        uploaded = await self.upload_path(path)
        result = await self.client(functions.messages.UploadMediaRequest(
            peer=self.destination, media=types.InputMediaUploadedPhoto(uploaded)))
        return utils.get_input_photo(result.photo)

    async def stage(self, message):
        self.check_stop()
        media = message.media
        if media is None or isinstance(media, (types.MessageMediaEmpty, types.MessageMediaWebPage)):
            # Web previews are requested again at send time, not cloned as old metadata.
            if not message.message:
                raise UploadProblem(f'ID {message.id}: empty non-media message cannot be uploaded.')
            return None
        if isinstance(media, (types.MessageMediaPhoto, types.MessageMediaDocument)):
            if getattr(media, 'ttl_seconds', None):
                raise UploadProblem(f'ID {message.id}: expiring media cannot be preserved as a permanent copy.')
            if isinstance(media, types.MessageMediaPhoto) and getattr(media, 'live_photo', False):
                raise UploadProblem(f'ID {message.id}: live photos are not supported by the upload fallback.')
            self.log(f'Downloading source ID {message.id}')
            if isinstance(media, types.MessageMediaPhoto):
                if not isinstance(media.photo, types.Photo):
                    raise UploadProblem(f'ID {message.id}: photo is unavailable.')
                suffix = '.jpg'
            else:
                if not isinstance(media.document, types.Document):
                    raise UploadProblem(f'ID {message.id}: document is unavailable.')
                suffix = mimetypes.guess_extension(media.document.mime_type) or '.bin'
            path = await self.client.download_media(
                message, file=str(self.directory / f'{message.id}-media{suffix}'), progress_callback=self.progress)
            if not path or not Path(path).is_file():
                raise UploadProblem(f'ID {message.id}: Telegram did not provide downloadable media.')
            if isinstance(media, types.MessageMediaDocument) and Path(path).stat().st_size != media.document.size:
                raise UploadProblem(f'ID {message.id}: downloaded file size does not match the source.')
            self.log(f'Uploading source ID {message.id}')
            uploaded = await self.upload_path(path)
            spoiler = bool(getattr(media, 'spoiler', False))
            cover = None
            if isinstance(media, types.MessageMediaPhoto):
                item = types.InputMediaUploadedPhoto(uploaded, spoiler=spoiler)
            else:
                thumb = await self.source_thumbnail(message)
                cover = await self.source_cover(message)
                attrs = copy.deepcopy(media.document.attributes)
                # Video/audio/sticker/file-name attributes are copied, rather than inferred
                # from a temporary extension. No recompression is performed locally.
                item = types.InputMediaUploadedDocument(
                    file=uploaded, mime_type=media.document.mime_type, attributes=attrs,
                    thumb=thumb, spoiler=spoiler, video_cover=cover,
                    video_timestamp=getattr(media, 'video_timestamp', None),
                    force_file=not any(isinstance(a, (types.DocumentAttributeVideo, types.DocumentAttributeAudio,
                                                     types.DocumentAttributeAnimated, types.DocumentAttributeSticker,
                                                     types.DocumentAttributeCustomEmoji)) for a in attrs))
            # Register the uploaded object first. Albums must reference registered media;
            # this call does NOT create a channel post.
            registered = await self.client(functions.messages.UploadMediaRequest(peer=self.destination, media=item))
            result = utils.get_input_media(registered)
            result.spoiler = spoiler
            if isinstance(result, types.InputMediaDocument):
                result.video_cover = cover
                result.video_timestamp = getattr(media, 'video_timestamp', None)
            return result
        if isinstance(media, types.MessageMediaContact):
            return types.InputMediaContact(media.phone_number, media.first_name, media.last_name, media.vcard)
        if isinstance(media, types.MessageMediaGeo):
            return types.InputMediaGeoPoint(utils.get_input_geo(media.geo))
        if isinstance(media, types.MessageMediaVenue):
            return types.InputMediaVenue(utils.get_input_geo(media.geo), media.title, media.address,
                                         media.provider, media.venue_id, media.venue_type)
        # Dice would roll again; polls would lose results. Do not call that preservation.
        raise UploadProblem(f'ID {message.id}: {type(media).__name__} cannot be faithfully reuploaded by this fallback.')


def build_request(destination, messages, media, random_ids):
    if not messages or len(messages) != len(media) or len(messages) != len(random_ids):
        raise UploadProblem('Inconsistent upload unit.')
    validate_album(messages)
    invert = bool(getattr(messages[0], 'invert_media', False))
    if len(messages) > 1:
        return functions.messages.SendMultiMediaRequest(
            peer=destination, silent=True, invert_media=invert,
            multi_media=[types.InputSingleMedia(item, m.message or '', random_id=rid, entities=m.entities or [])
                         for m, item, rid in zip(messages, media, random_ids)])
    message = messages[0]
    common = dict(peer=destination, message=message.message or '', random_id=random_ids[0],
                  entities=message.entities or [], silent=True, invert_media=invert)
    if media[0] is None:
        return functions.messages.SendMessageRequest(
            **common, no_webpage=not isinstance(message.media, types.MessageMediaWebPage))
    return functions.messages.SendMediaRequest(**common, media=media[0])


def extract_ids(result, random_ids):
    mapping = {u.random_id: u.id for u in getattr(result, 'updates', []) if isinstance(u, types.UpdateMessageID)}
    if len(random_ids) == 1 and isinstance(result, types.UpdateShortSentMessage):
        return [result.id]
    if not all(r in mapping for r in random_ids):
        raise UploadProblem('Send response lacks a complete ID mapping; delivery must be reviewed before retrying.')
    return [mapping[r] for r in random_ids]
