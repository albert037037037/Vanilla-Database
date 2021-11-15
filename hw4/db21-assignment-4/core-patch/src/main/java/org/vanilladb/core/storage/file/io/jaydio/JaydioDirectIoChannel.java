/*******************************************************************************
 * Copyright 2016, 2017 vanilladb.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.vanilladb.core.storage.file.io.jaydio;

import java.io.File;
import java.io.IOException;

import org.vanilladb.core.storage.file.io.IoBuffer;
import org.vanilladb.core.storage.file.io.IoChannel;

import net.smacke.jaydio.buffer.AlignedDirectByteBuffer;
import net.smacke.jaydio.channel.BufferedChannel;
import net.smacke.jaydio.channel.DirectIoByteChannel;

public class JaydioDirectIoChannel implements IoChannel {

	private BufferedChannel<AlignedDirectByteBuffer> fileChannel;

	public JaydioDirectIoChannel(File file) throws IOException {
		fileChannel = DirectIoByteChannel.getChannel(file, false);
	}

	@Override
	public int read(IoBuffer buffer, long position) throws IOException {
		JaydioDirectByteBuffer jaydioBuffer = (JaydioDirectByteBuffer) buffer;
		return fileChannel.read(jaydioBuffer.getAlignedDirectByteBuffer(), position);
	}

	@Override
	public int write(IoBuffer buffer, long position) throws IOException {
		JaydioDirectByteBuffer jaydioBuffer = (JaydioDirectByteBuffer) buffer;
		return fileChannel.write(jaydioBuffer.getAlignedDirectByteBuffer(), position);
	}

	@Override
	public long append(IoBuffer buffer) throws IOException {
		JaydioDirectByteBuffer jaydioBuffer = (JaydioDirectByteBuffer) buffer;
		fileChannel.write(jaydioBuffer.getAlignedDirectByteBuffer(), size());
		return size();
	}

	@Override
	public long size() throws IOException {
		return fileChannel.size();
	}

	@Override
	public void close() throws IOException {
		fileChannel.close();
	}
}
