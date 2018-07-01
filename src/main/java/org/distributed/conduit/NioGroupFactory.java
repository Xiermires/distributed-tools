package org.distributed.conduit;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

public interface NioGroupFactory {

    EventLoopGroup createParentGroup();

    EventLoopGroup createChildrenGroup();

    public static final NioGroupFactory defau1t = new NioGroupFactory() {
	@Override
	public EventLoopGroup createParentGroup() {
	    return new NioEventLoopGroup(1);
	}

	@Override
	public EventLoopGroup createChildrenGroup() {
	    return new NioEventLoopGroup();
	}
    };

    public static final NioGroupFactory minimal = new NioGroupFactory() {
	@Override
	public EventLoopGroup createParentGroup() {
	    return new NioEventLoopGroup(1);
	}

	@Override
	public EventLoopGroup createChildrenGroup() {
	    return new NioEventLoopGroup(1);
	}
    };

    public static NioGroupFactory configurable(int parentGroupSize, int childrenGroupSize) {
	return new NioGroupFactory() {
	    @Override
	    public EventLoopGroup createParentGroup() {
		return new NioEventLoopGroup(parentGroupSize);
	    }

	    @Override
	    public EventLoopGroup createChildrenGroup() {
		return childrenGroupSize > 0 ? new NioEventLoopGroup(childrenGroupSize) : new NioEventLoopGroup();
	    }
	};
    }
}
