/**
 * collectd - src/zeromq.c
 * Copyright (C) 2005-2010  Florian octo Forster
 * Copyright (C) 2009       Aman Gupta
 * Copyright (C) 2010       Julien Ammous
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; only version 2 of the License is applicable.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
 *
 * Authors:
 *   Florian octo Forster <octo at verplant.org>
 *   Aman Gupta <aman at tmm1.net>
 *   Julien Ammous
 **/

//
// mostly copy/pasted from network.c ...
// this whole file will be dropped as soon as
// it can be replaced by network_buffer library
//

static value_list_t     send_buffer_vl = VALUE_LIST_STATIC;

static uint64_t stats_values_not_dispatched = 0;
static uint64_t stats_values_dispatched = 0;

/*                      1 1 1 1 1 1 1 1 1 1 2 2 2 2 2 2 2 2 2 2 3 3
 *  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 * +-------+-----------------------+-------------------------------+
 * ! Ver.  !                       ! Length                        !
 * +-------+-----------------------+-------------------------------+
 */
struct part_header_s
{
	uint16_t type;
	uint16_t length;
};
typedef struct part_header_s part_header_t;

// we do not want to crypt here
#undef HAVE_LIBGCRYPT

static _Bool check_receive_okay (const value_list_t *vl) /* {{{ */
{
  uint64_t time_sent = 0;
  int status;

  status = uc_meta_data_get_unsigned_int (vl,
      "zeromq:time_sent", &time_sent);

  /* This is a value we already sent. Don't allow it to be received again in
   * order to avoid looping. */
  if ((status == 0) && (time_sent >= ((uint64_t) vl->time)))
    return (0);

  return (1);
} /* }}} _Bool check_receive_okay */

static int network_dispatch_values (value_list_t *vl, /* {{{ */
    const char *username)
{
  int status;
  
  // DEBUG("host: %s", vl->host);
  // DEBUG("plugin: %s", vl->plugin);
  // DEBUG("plugin_instance: %s", vl->plugin_instance);
  // DEBUG("type: %s", vl->type);
  // DEBUG("type_instance: %s", vl->type_instance);

  if ((vl->time <= 0)
      || (strlen (vl->host) <= 0)
      || (strlen (vl->plugin) <= 0)
      || (strlen (vl->type) <= 0))
    return (-EINVAL);
  
  if (!check_receive_okay (vl))
  {
#if COLLECT_DEBUG
    char name[6*DATA_MAX_NAME_LEN];
    FORMAT_VL (name, sizeof (name), vl);
    name[sizeof (name) - 1] = 0;
    DEBUG ("network plugin: network_dispatch_values: "
	"NOT dispatching %s.", name);
#endif
    stats_values_not_dispatched++;
    return (0);
  }

  assert (vl->meta == NULL);

  vl->meta = meta_data_create ();
  if (vl->meta == NULL)
  {
    ERROR ("network plugin: meta_data_create failed.");
    return (-ENOMEM);
  }

  status = meta_data_add_boolean (vl->meta, "zeromq:received", 1);
  if (status != 0)
  {
    ERROR ("network plugin: meta_data_add_boolean failed.");
    meta_data_destroy (vl->meta);
    vl->meta = NULL;
    return (status);
  }

  if (username != NULL)
  {
    status = meta_data_add_string (vl->meta, "zeromq:username", username);
    if (status != 0)
    {
      ERROR ("network plugin: meta_data_add_string failed.");
      meta_data_destroy (vl->meta);
      vl->meta = NULL;
      return (status);
    }
  }
  
  // DEBUG("dispatching %d values", vl->values_len);
  plugin_dispatch_values (vl);
  stats_values_dispatched++;

  meta_data_destroy (vl->meta);
  vl->meta = NULL;

  return (0);
} /* }}} int network_dispatch_values */

static int write_part_values (char **ret_buffer, int *ret_buffer_len, const data_set_t *ds, const value_list_t *vl)
{
  char *packet_ptr;
  int packet_len;
  int num_values;

  part_header_t pkg_ph;
  uint16_t      pkg_num_values;
  uint8_t      *pkg_values_types;
  value_t      *pkg_values;

  int offset;
  int i;

  num_values = vl->values_len;
  packet_len = sizeof (part_header_t) + sizeof (uint16_t)
    + (num_values * sizeof (uint8_t))
    + (num_values * sizeof (value_t));

  if (*ret_buffer_len < packet_len)
    return (-1);

  pkg_values_types = (uint8_t *) malloc (num_values * sizeof (uint8_t));
  if (pkg_values_types == NULL)
  {
    ERROR ("network plugin: write_part_values: malloc failed.");
    return (-1);
  }

  pkg_values = (value_t *) malloc (num_values * sizeof (value_t));
  if (pkg_values == NULL)
  {
    free (pkg_values_types);
    ERROR ("network plugin: write_part_values: malloc failed.");
    return (-1);
  }

  pkg_ph.type = htons (TYPE_VALUES);
  pkg_ph.length = htons (packet_len);

  pkg_num_values = htons ((uint16_t) vl->values_len);

  for (i = 0; i < num_values; i++)
  {
    pkg_values_types[i] = (uint8_t) ds->ds[i].type;
    switch (ds->ds[i].type)
    {
      case DS_TYPE_COUNTER:
        pkg_values[i].counter = htonll (vl->values[i].counter);
        break;

      case DS_TYPE_GAUGE:
        pkg_values[i].gauge = htond (vl->values[i].gauge);
        break;

      case DS_TYPE_DERIVE:
        pkg_values[i].derive = htonll (vl->values[i].derive);
        break;

      case DS_TYPE_ABSOLUTE:
        pkg_values[i].absolute = htonll (vl->values[i].absolute);
        break;

      default:
        free (pkg_values_types);
        free (pkg_values);
        ERROR ("network plugin: write_part_values: "
            "Unknown data source type: %i",
            ds->ds[i].type);
        return (-1);
    } /* switch (ds->ds[i].type) */
  } /* for (num_values) */

  /*
   * Use `memcpy' to write everything to the buffer, because the pointer
   * may be unaligned and some architectures, such as SPARC, can't handle
   * that.
   */
  packet_ptr = *ret_buffer;
  offset = 0;
  memcpy (packet_ptr + offset, &pkg_ph, sizeof (pkg_ph));
  offset += sizeof (pkg_ph);
  memcpy (packet_ptr + offset, &pkg_num_values, sizeof (pkg_num_values));
  offset += sizeof (pkg_num_values);
  memcpy (packet_ptr + offset, pkg_values_types, num_values * sizeof (uint8_t));
  offset += num_values * sizeof (uint8_t);
  memcpy (packet_ptr + offset, pkg_values, num_values * sizeof (value_t));
  offset += num_values * sizeof (value_t);

  assert (offset == packet_len);

  *ret_buffer = packet_ptr + packet_len;
  *ret_buffer_len -= packet_len;

  free (pkg_values_types);
  free (pkg_values);

  return (0);
} /* int write_part_values */

static int write_part_number (char **ret_buffer, int *ret_buffer_len,
    int type, uint64_t value)
{
  char *packet_ptr;
  int packet_len;

  part_header_t pkg_head;
  uint64_t pkg_value;
  
  int offset;

  packet_len = sizeof (pkg_head) + sizeof (pkg_value);

  if (*ret_buffer_len < packet_len)
    return (-1);

  pkg_head.type = htons (type);
  pkg_head.length = htons (packet_len);
  pkg_value = htonll (value);

  packet_ptr = *ret_buffer;
  offset = 0;
  memcpy (packet_ptr + offset, &pkg_head, sizeof (pkg_head));
  offset += sizeof (pkg_head);
  memcpy (packet_ptr + offset, &pkg_value, sizeof (pkg_value));
  offset += sizeof (pkg_value);

  assert (offset == packet_len);

  *ret_buffer = packet_ptr + packet_len;
  *ret_buffer_len -= packet_len;

  return (0);
} /* int write_part_number */

static int write_part_string (char **ret_buffer, int *ret_buffer_len,
    int type, const char *str, int str_len)
{
  char *buffer;
  int buffer_len;

  uint16_t pkg_type;
  uint16_t pkg_length;

  int offset;

  buffer_len = 2 * sizeof (uint16_t) + str_len + 1;
  if (*ret_buffer_len < buffer_len)
    return (-1);

  pkg_type = htons (type);
  pkg_length = htons (buffer_len);

  buffer = *ret_buffer;
  offset = 0;
  memcpy (buffer + offset, (void *) &pkg_type, sizeof (pkg_type));
  offset += sizeof (pkg_type);
  memcpy (buffer + offset, (void *) &pkg_length, sizeof (pkg_length));
  offset += sizeof (pkg_length);
  memcpy (buffer + offset, str, str_len);
  offset += str_len;
  memset (buffer + offset, '\0', 1);
  offset += 1;

  assert (offset == buffer_len);

  *ret_buffer = buffer + buffer_len;
  *ret_buffer_len -= buffer_len;

  return (0);
} /* int write_part_string */

static int parse_part_values (void **ret_buffer, size_t *ret_buffer_len,
    value_t **ret_values, int *ret_num_values)
{
  char *buffer = *ret_buffer;
  size_t buffer_len = *ret_buffer_len;

  uint16_t tmp16;
  size_t exp_size;
  int   i;

  uint16_t pkg_length;
  uint16_t pkg_type;
  uint16_t pkg_numval;

  uint8_t *pkg_types;
  value_t *pkg_values;

  if (buffer_len < 15)
  {
    NOTICE ("network plugin: packet is too short: "
        "buffer_len = %zu", buffer_len);
    return (-1);
  }

  memcpy ((void *) &tmp16, buffer, sizeof (tmp16));
  buffer += sizeof (tmp16);
  pkg_type = ntohs (tmp16);

  memcpy ((void *) &tmp16, buffer, sizeof (tmp16));
  buffer += sizeof (tmp16);
  pkg_length = ntohs (tmp16);

  memcpy ((void *) &tmp16, buffer, sizeof (tmp16));
  buffer += sizeof (tmp16);
  pkg_numval = ntohs (tmp16);

  assert (pkg_type == TYPE_VALUES);

  exp_size = 3 * sizeof (uint16_t)
    + pkg_numval * (sizeof (uint8_t) + sizeof (value_t));
  if ((buffer_len < 0) || (buffer_len < exp_size))
  {
    WARNING ("network plugin: parse_part_values: "
        "Packet too short: "
        "Chunk of size %zu expected, "
        "but buffer has only %zu bytes left.",
        exp_size, buffer_len);
    return (-1);
  }

  if (pkg_length != exp_size)
  {
    WARNING ("network plugin: parse_part_values: "
        "Length and number of values "
        "in the packet don't match.");
    return (-1);
  }

  pkg_types = (uint8_t *) malloc (pkg_numval * sizeof (uint8_t));
  pkg_values = (value_t *) malloc (pkg_numval * sizeof (value_t));
  if ((pkg_types == NULL) || (pkg_values == NULL))
  {
    sfree (pkg_types);
    sfree (pkg_values);
    ERROR ("network plugin: parse_part_values: malloc failed.");
    return (-1);
  }

  memcpy ((void *) pkg_types, (void *) buffer, pkg_numval * sizeof (uint8_t));
  buffer += pkg_numval * sizeof (uint8_t);
  memcpy ((void *) pkg_values, (void *) buffer, pkg_numval * sizeof (value_t));
  buffer += pkg_numval * sizeof (value_t);

  for (i = 0; i < pkg_numval; i++)
  {
    switch (pkg_types[i])
    {
      case DS_TYPE_COUNTER:
        pkg_values[i].counter = (counter_t) ntohll (pkg_values[i].counter);
        break;

      case DS_TYPE_GAUGE:
        pkg_values[i].gauge = (gauge_t) ntohd (pkg_values[i].gauge);
        break;

      case DS_TYPE_DERIVE:
        pkg_values[i].derive = (derive_t) ntohll (pkg_values[i].derive);
        break;

      case DS_TYPE_ABSOLUTE:
        pkg_values[i].absolute = (absolute_t) ntohll (pkg_values[i].absolute);
        break;

      default:
        NOTICE ("network plugin: parse_part_values: "
      "Don't know how to handle data source type %"PRIu8,
      pkg_types[i]);
        sfree (pkg_types);
        sfree (pkg_values);
        return (-1);
    } /* switch (pkg_types[i]) */
  }

  *ret_buffer     = buffer;
  *ret_buffer_len = buffer_len - pkg_length;
  *ret_num_values = pkg_numval;
  *ret_values     = pkg_values;

  sfree (pkg_types);

  return (0);
} /* int parse_part_values */

static int add_to_buffer (char *buffer, int buffer_size, /* {{{ */
    value_list_t *vl_def,
    const data_set_t *ds, const value_list_t *vl)
{
  char *buffer_orig = buffer;
  
  if (write_part_string (&buffer, &buffer_size, TYPE_HOST, vl->host, strlen (vl->host)) != 0)
    return (-1);
  
  if (write_part_number (&buffer, &buffer_size, TYPE_TIME, (uint64_t) vl->time))
    return (-1);
  
  if (write_part_number (&buffer, &buffer_size, TYPE_INTERVAL, (uint64_t) vl->interval))
    return (-1);
    
  if (write_part_string (&buffer, &buffer_size, TYPE_PLUGIN, vl->plugin, strlen (vl->plugin)) != 0)
    return (-1);
  
  if (write_part_string (&buffer, &buffer_size, TYPE_PLUGIN_INSTANCE, vl->plugin_instance, strlen (vl->plugin_instance)) != 0)
    return (-1);
  
  if (write_part_string (&buffer, &buffer_size, TYPE_TYPE, vl->type, strlen (vl->type)) != 0)
    return (-1);
  
  if (write_part_string (&buffer, &buffer_size, TYPE_TYPE_INSTANCE, vl->type_instance, strlen (vl->type_instance)) != 0)
    return (-1);
  
  if (write_part_values (&buffer, &buffer_size, ds, vl) != 0)
    return (-1);

  return (buffer - buffer_orig);
} /* }}} int add_to_buffer */

static int parse_part_number (void **ret_buffer, size_t *ret_buffer_len,
    uint64_t *value)
{
  char *buffer = *ret_buffer;
  size_t buffer_len = *ret_buffer_len;

  uint16_t tmp16;
  uint64_t tmp64;
  size_t exp_size = 2 * sizeof (uint16_t) + sizeof (uint64_t);

  uint16_t pkg_length;
  uint16_t pkg_type;

  if ((buffer_len < 0) || ((size_t) buffer_len < exp_size))
  {
    WARNING ("network plugin: parse_part_number: "
        "Packet too short: "
        "Chunk of size %zu expected, "
        "but buffer has only %zu bytes left.",
        exp_size, buffer_len);
    return (-1);
  }

  memcpy ((void *) &tmp16, buffer, sizeof (tmp16));
  buffer += sizeof (tmp16);
  pkg_type = ntohs (tmp16);

  memcpy ((void *) &tmp16, buffer, sizeof (tmp16));
  buffer += sizeof (tmp16);
  pkg_length = ntohs (tmp16);

  memcpy ((void *) &tmp64, buffer, sizeof (tmp64));
  buffer += sizeof (tmp64);
  *value = ntohll (tmp64);

  *ret_buffer = buffer;
  *ret_buffer_len = buffer_len - pkg_length;

  return (0);
} /* int parse_part_number */

static int parse_part_string (void **ret_buffer, size_t *ret_buffer_len,
    char *output, int output_len)
{
  char *buffer = *ret_buffer;
  size_t buffer_len = *ret_buffer_len;

  uint16_t tmp16;
  size_t header_size = 2 * sizeof (uint16_t);

  uint16_t pkg_length;
  uint16_t pkg_type;

  if ((buffer_len < 0) || (buffer_len < header_size))
  {
    WARNING ("network plugin: parse_part_string: "
        "Packet too short: "
        "Chunk of at least size %zu expected, "
        "but buffer has only %zu bytes left.",
        header_size, buffer_len);
    return (-1);
  }

  memcpy ((void *) &tmp16, buffer, sizeof (tmp16));
  buffer += sizeof (tmp16);
  pkg_type = ntohs (tmp16);

  memcpy ((void *) &tmp16, buffer, sizeof (tmp16));
  buffer += sizeof (tmp16);
  pkg_length = ntohs (tmp16);

  /* Check that packet fits in the input buffer */
  if (pkg_length > buffer_len)
  {
    WARNING ("network plugin: parse_part_string: "
        "Packet too big: "
        "Chunk of size %"PRIu16" received, "
        "but buffer has only %zu bytes left.",
        pkg_length, buffer_len);
    return (-1);
  }

  /* Check that pkg_length is in the valid range */
  if (pkg_length <= header_size)
  {
    WARNING ("network plugin: parse_part_string: "
        "Packet too short: "
        "Header claims this packet is only %hu "
        "bytes long.", pkg_length);
    return (-1);
  }

  /* Check that the package data fits into the output buffer.
   * The previous if-statement ensures that:
   * `pkg_length > header_size' */
  if ((output_len < 0)
      || ((size_t) output_len < ((size_t) pkg_length - header_size)))
  {
    WARNING ("network plugin: parse_part_string: "
        "Output buffer too small.");
    return (-1);
  }

  /* All sanity checks successfull, let's copy the data over */
  output_len = pkg_length - header_size;
  memcpy ((void *) output, (void *) buffer, output_len);
  buffer += output_len;

  /* For some very weird reason '\0' doesn't do the trick on SPARC in
   * this statement. */
  if (output[output_len - 1] != 0)
  {
    WARNING ("network plugin: parse_part_string: "
        "Received string does not end "
        "with a NULL-byte.");
    return (-1);
  }

  *ret_buffer = buffer;
  *ret_buffer_len = buffer_len - pkg_length;

  return (0);
} /* int parse_part_string */


static int parse_packet (void *se, /* {{{ */
    void *buffer, size_t buffer_size, int flags,
    const char *username)
{
  int status;

  value_list_t vl = VALUE_LIST_INIT;
  notification_t n;

#if HAVE_LIBGCRYPT
  int packet_was_signed = (flags & PP_SIGNED);
        int packet_was_encrypted = (flags & PP_ENCRYPTED);
  int printed_ignore_warning = 0;
#endif /* HAVE_LIBGCRYPT */


  memset (&vl, '\0', sizeof (vl));
  memset (&n, '\0', sizeof (n));
  status = 0;

  while ((status == 0) && (0 < buffer_size)
      && ((unsigned int) buffer_size > sizeof (part_header_t)))
  {
    uint16_t pkg_length;
    uint16_t pkg_type;

    memcpy ((void *) &pkg_type,
        (void *) buffer,
        sizeof (pkg_type));
    memcpy ((void *) &pkg_length,
        (void *) (buffer + sizeof (pkg_type)),
        sizeof (pkg_length));

    pkg_length = ntohs (pkg_length);
    pkg_type = ntohs (pkg_type);

    if (pkg_length > buffer_size)
      break;
    /* Ensure that this loop terminates eventually */
    if (pkg_length < (2 * sizeof (uint16_t)))
      break;

    if (pkg_type == TYPE_ENCR_AES256)
    {
      // status = parse_part_encr_aes256 (se,
      //     &buffer, &buffer_size, flags);
      // if (status != 0)
      // {
        ERROR ("network plugin: Decrypting AES256 "
            "part failed "
            "with status %i.", status);
        break;
      // }
    }
#if HAVE_LIBGCRYPT
    else if ((se->data.server.security_level == SECURITY_LEVEL_ENCRYPT)
        && (packet_was_encrypted == 0))
    {
      if (printed_ignore_warning == 0)
      {
        INFO ("network plugin: Unencrypted packet or "
            "part has been ignored.");
        printed_ignore_warning = 1;
      }
      buffer = ((char *) buffer) + pkg_length;
      continue;
    }
#endif /* HAVE_LIBGCRYPT */
    else if (pkg_type == TYPE_SIGN_SHA256)
    {
      // status = parse_part_sign_sha256 (se,
      //                                   &buffer, &buffer_size, flags);
      // if (status != 0)
      // {
        ERROR ("network plugin: Verifying HMAC-SHA-256 "
            "signature failed "
            "with status %i.", status);
        break;
      // }
    }
#if HAVE_LIBGCRYPT
    else if ((se->data.server.security_level == SECURITY_LEVEL_SIGN)
        && (packet_was_encrypted == 0)
        && (packet_was_signed == 0))
    {
      if (printed_ignore_warning == 0)
      {
        INFO ("network plugin: Unsigned packet or "
            "part has been ignored.");
        printed_ignore_warning = 1;
      }
      buffer = ((char *) buffer) + pkg_length;
      continue;
    }
#endif /* HAVE_LIBGCRYPT */
    else if (pkg_type == TYPE_VALUES)
    {
      status = parse_part_values (&buffer, &buffer_size,
          &vl.values, &vl.values_len);
      if (status != 0)
        break;

      network_dispatch_values (&vl, username);

      sfree (vl.values);
    }
    else if (pkg_type == TYPE_TIME)
    {
      uint64_t tmp = 0;
      status = parse_part_number (&buffer, &buffer_size,
          &tmp);
      if (status == 0)
      {
        vl.time = (time_t) tmp;
        n.time = (time_t) tmp;
      }
    }
    else if (pkg_type == TYPE_INTERVAL)
    {
      uint64_t tmp = 0;
      status = parse_part_number (&buffer, &buffer_size,
          &tmp);
      if (status == 0)
        vl.interval = (int) tmp;
    }
    else if (pkg_type == TYPE_HOST)
    {
      status = parse_part_string (&buffer, &buffer_size,
          vl.host, sizeof (vl.host));
      if (status == 0)
        sstrncpy (n.host, vl.host, sizeof (n.host));
    }
    else if (pkg_type == TYPE_PLUGIN)
    {
      status = parse_part_string (&buffer, &buffer_size,
          vl.plugin, sizeof (vl.plugin));
      if (status == 0)
        sstrncpy (n.plugin, vl.plugin,
            sizeof (n.plugin));
    }
    else if (pkg_type == TYPE_PLUGIN_INSTANCE)
    {
      status = parse_part_string (&buffer, &buffer_size,
          vl.plugin_instance,
          sizeof (vl.plugin_instance));
      if (status == 0)
        sstrncpy (n.plugin_instance,
            vl.plugin_instance,
            sizeof (n.plugin_instance));
    }
    else if (pkg_type == TYPE_TYPE)
    {
      status = parse_part_string (&buffer, &buffer_size,
          vl.type, sizeof (vl.type));
      if (status == 0)
        sstrncpy (n.type, vl.type, sizeof (n.type));
    }
    else if (pkg_type == TYPE_TYPE_INSTANCE)
    {
      status = parse_part_string (&buffer, &buffer_size,
          vl.type_instance,
          sizeof (vl.type_instance));
      if (status == 0)
        sstrncpy (n.type_instance, vl.type_instance,
            sizeof (n.type_instance));
    }
    else if (pkg_type == TYPE_MESSAGE)
    {
      status = parse_part_string (&buffer, &buffer_size,
          n.message, sizeof (n.message));

      if (status != 0)
      {
        /* do nothing */
      }
      else if ((n.severity != NOTIF_FAILURE)
          && (n.severity != NOTIF_WARNING)
          && (n.severity != NOTIF_OKAY))
      {
        INFO ("network plugin: "
            "Ignoring notification with "
            "unknown severity %i.",
            n.severity);
      }
      else if (n.time <= 0)
      {
        INFO ("network plugin: "
            "Ignoring notification with "
            "time == 0.");
      }
      else if (strlen (n.message) <= 0)
      {
        INFO ("network plugin: "
            "Ignoring notification with "
            "an empty message.");
      }
      else
      {
        plugin_dispatch_notification (&n);
      }
    }
    else if (pkg_type == TYPE_SEVERITY)
    {
      uint64_t tmp = 0;
      status = parse_part_number (&buffer, &buffer_size,
          &tmp);
      if (status == 0)
        n.severity = (int) tmp;
    }
    else
    {
      DEBUG ("network plugin: parse_packet: Unknown part"
          " type: 0x%04hx", pkg_type);
      buffer = ((char *) buffer) + pkg_length;
    }
  } /* while (buffer_size > sizeof (part_header_t)) */

  if (status == 0 && buffer_size > 0)
    WARNING ("network plugin: parse_packet: Received truncated "
        "packet, try increasing `MaxPacketSize'");

  return (status);
} /* }}} int parse_packet */

